use anyhow::{Context, anyhow};
use clap::Parser;
use std::{io::Write, process::ExitStatus};

#[derive(Parser)]
#[command(version = option_env!("IHN_HPC_SBATCH_ARRAY_VERSION").unwrap_or("debug"), verbatim_doc_comment)]
struct Args {
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
    /// IMAGE specifies the podman image for the container. A short-hand identifier, e.g.
    /// "freesurfer", may be used for known images. Otherwise IMAGE is passed directly to
    /// podman-run.
    #[arg(value_parser = parse_image)]
    image: Image,
    /// Command to execute inside the container
    ///
    /// COMMAND specifies the command executed inside the container. If COMMAND has a shell script
    /// extension (.sh) and exists on the host it is treated as a user-defined shell script and
    /// mounted inside the container.
    command: String,
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    command_args: Vec<String>,
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
    sbatch_command.arg("--gres=gpu:a100:1");
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
srun --ntasks=1 podman run --rm \
    --security-opt=label=disable \
    --device=nvidia.com/gpu=all \
    -v \"$HOME\":\"$HOME\" \
    -e HPC_HOME=\"$HOME\" \
    -v /mnt/home/shared/:/mnt/home/shared/ \
    {command_volume_arg} \
    {additional_podman_args} \
    --authfile /mnt/apps/etc/auth.json \
    --entrypoint {command} \
    {podman_args} \
    {image} {command_args}",
            additional_podman_args = podman_args_for_image(&args.image),
            podman_args = args.podman_args.unwrap_or("".to_string()),
            image = qualified_image_name(args.image, args.tag),
            command_args = args.command_args.join(" ")
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
