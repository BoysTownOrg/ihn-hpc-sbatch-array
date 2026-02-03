use anyhow::{Context, anyhow};
use clap::Parser;
use std::{io::Write, process::ExitStatus};

#[derive(Parser)]
#[command(version = option_env!("IHN_HPC_SBATCH_ARRAY_VERSION").unwrap_or("debug"), verbatim_doc_comment)]
/// A program wrapping Slurm's "sbatch" command tailored for IHN's HPC cluster.
///
/// ihn-hpc-sbatch-array wraps Slurm's "sbatch" command and creates a job array
/// from a text file. The text file (COMMAND_ARG_PATH) contains one nonempty line
/// for each job in the array. Each line is trimmed and passed to COMMAND as the
/// one and only argument. The COMMAND is executed inside a podman container. The
/// user's home directory (/mnt/home/username/) and shared directory
/// (/mnt/home/shared/) are mounted inside the container at the same locations.
///
/// EXAMPLE
/// Given the file "arg.txt" with contents:
/// M68123456
/// M68654321
/// M68112233
///
/// and file "script.sh" with contents:
/// #!/bin/bash
/// SUBJECT=$1
/// HOME=/mnt/home/username/
/// recon-all -i "$HOME"/"$SUBJECT".nii -sd "$HOME"/subjects/ -subjid "$SUBJECT" -all
///
/// The command:
/// $ ihn-hpc-sbatch-array freesurfer /path/to/script.sh /path/to/arg.txt
///
/// creates a three element job array each running a freesurfer container that
/// executes "script.sh". In this case, the script receives an argument
/// representing the subject ID and calls "recon-all" for the given subject.
struct Args {
    /// The maximum number of simultaneous tasks
    #[arg(long, default_value_t = 16)]
    max_tasks: i32,
    /// Podman image tag - ignored when IMAGE is fully qualified
    #[arg(long)]
    tag: Option<String>,
    /// Additional args to sbatch
    #[arg(long, allow_hyphen_values = true)]
    sbatch_args: Option<String>,
    /// Additional args to podman
    #[arg(long, allow_hyphen_values = true)]
    podman_args: Option<String>,
    /// Podman image - short-hand identifier or qualified name
    ///
    /// IMAGE specifies the podman image used for each container. A short-hand identifier, e.g.
    /// "freesurfer", may be used for known images. Otherwise IMAGE is passed directly to
    /// podman-run for each job.
    #[arg(value_parser = parse_image)]
    image: Image,
    /// Command to execute inside each container
    ///
    /// COMMAND specifies the command executed inside each container. If COMMAND has a shell script
    /// extension (.sh) and exists on the host it is treated as a user-defined shell script and
    /// mounted inside each container.
    command: String,
    /// Path to a plaintext file containing one argument per line - the one
    /// argument passed to COMMAND for each array job
    command_arg_path: std::path::PathBuf,
}

#[derive(Clone)]
enum Image {
    Freesurfer,
    QualifiedName(String),
}

fn parse_image(s: &str) -> anyhow::Result<Image> {
    match s.to_lowercase().as_str() {
        "freesurfer" => Ok(Image::Freesurfer),
        _ => Ok(Image::QualifiedName(s.to_string())),
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(status) => {
            if status.success() {
                std::process::ExitCode::SUCCESS
            } else {
                eprintln!("ERROR: Something went wrong...");
                std::process::ExitCode::FAILURE
            }
        }
        Err(what) => {
            eprintln!("ERROR: {what:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<ExitStatus> {
    let args = Args::parse();
    let command_arg_path = std::fs::read_to_string(&args.command_arg_path).with_context(|| {
        format!(
            "Unable to read command argument file, {:?}",
            args.command_arg_path
        )
    })?;
    let command_arg_vec = command_arg_path
        .lines()
        .map(|line| format!("\"{}\"", line.trim()))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let (command, command_volume_arg) = {
        let command_path = std::path::Path::new(&args.command);
        if command_path.exists() && command_path.extension().is_some_and(|ext| ext == "sh") {
            let mounted_command_path = std::fs::canonicalize(command_path).with_context(||
                format!("It looks like \"{command}\" is a shell script, but a canonical path cannot be determined for mounting in each container", command=args.command)
            )?.to_str().with_context(||format!("\"{command}\" is not valid UTF-8", command=args.command))?.to_string();
            (
                mounted_command_path.clone(),
                format!(
                    "-v {}:{}",
                    mounted_command_path.clone(),
                    mounted_command_path
                ),
            )
        } else {
            (args.command, "".to_string())
        }
    };
    let mut sbatch_command = std::process::Command::new("sbatch");
    sbatch_command.arg(format!(
        "--array=0-{}%{}",
        command_arg_vec.len() - 1,
        args.max_tasks
    ));
    if let Some(args) = args.sbatch_args {
        sbatch_command.args(args.split_whitespace());
    }
    let mut sbatch_child = sbatch_command
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("Unable to invoke sbatch")?;
    if let Some(mut stdin) = sbatch_child.stdin.take() {
        writeln!(
            stdin,
            "#!/bin/bash
set -u
export TMPDIR=/ssd/home/$USER/TEMP
COMMAND_ARGS=(
{command_args}
)
srun --ntasks=1 podman run --rm \
    -v \"$HOME\":\"$HOME\" \
    -v /mnt/home/shared/:/mnt/home/shared/ \
    {command_volume_arg} \
    {additional_podman_args} \
    --authfile /mnt/apps/etc/auth.json \
    --entrypoint {command} \
    {podman_args} \
    {image} \"${{COMMAND_ARGS[$SLURM_ARRAY_TASK_ID]}}\"",
            command_args = command_arg_vec.join("\n"),
            additional_podman_args = podman_args_for_image(&args.image),
            podman_args = args.podman_args.unwrap_or("".to_string()),
            image = qualified_image_name(args.image, args.tag)
        )?;
    } else {
        return Err(anyhow!("Unable to take stdin of sbatch"));
    }
    Ok(sbatch_child.wait()?)
}

fn podman_args_for_image(c: &Image) -> &'static str {
    match c {
        Image::Freesurfer => {
            "\
-v /mnt/apps/etc/fs_license.txt:/usr/local/freesurfer/.license:ro \
-v /opt/matlab/runtime/R2019b/v97/:/usr/local/freesurfer/MCRv97 \
-e FS_LICENSE=/usr/local/freesurfer/.license"
        }
        Image::QualifiedName(_) => "",
    }
}

fn qualified_image_name(image: Image, tag: Option<String>) -> String {
    match image {
        Image::Freesurfer => format!(
            "docker.io/freesurfer/freesurfer:{}",
            tag.unwrap_or_else(|| "7.3.2".to_string())
        ),
        Image::QualifiedName(n) => {
            if let Some(t) = tag {
                eprintln!("WARN: ignoring tag \"{t}\"");
            }
            n
        }
    }
}
