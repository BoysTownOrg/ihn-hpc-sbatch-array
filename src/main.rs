use anyhow::{Context, anyhow};
use clap::Parser;
use std::{io::Write, process::ExitStatus};

#[derive(Parser)]
#[command(version = option_env!("IHN_HPC_SBATCH_ARRAY_VERSION").unwrap_or("debug"))]
struct Args {
    #[arg(long)]
    image: Option<String>,
    #[arg(long)]
    max_tasks: Option<String>,
    #[arg(long)]
    sbatch_args: Option<String>,
    #[arg(long)]
    podman_args: Option<String>,
    container: Container,
    command: String,
    command_arg_path: std::path::PathBuf,
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum Container {
    Freesurfer,
    Other,
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
            let mounted_command_path = format!("/{}", args.command);
            (
                mounted_command_path.clone(),
                format!("-v {}:{}", args.command, mounted_command_path),
            )
        } else {
            (args.command, "".to_string())
        }
    };
    let image = match container_image(args.container) {
        Some(image) => image.to_string(),
        None => args
            .image
            .context("--image must be specified if \"other\" container is chosen")?,
    };
    let mut sbatch = std::process::Command::new("sbatch")
        .arg(format!(
            "--array=0-{}%{}",
            command_arg_vec.len() - 1,
            args.max_tasks.unwrap_or("16".to_string())
        ))
        .args(args.sbatch_args)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("Unable to invoke sbatch")?;
    if let Some(mut stdin) = sbatch.stdin.take() {
        writeln!(
            stdin,
            "#!/bin/bash
set -u
export TMPDIR=/ssd/home/$USER/TEMP
export REGISTRY_AUTH_FILE=/mnt/apps/etc/auth.json
INPUT=(
{input_array}
)
srun --ntasks=1 podman run --rm \
    {command_volume_arg} \
    {volume_args} \
    --entrypoint {command} \
    {podman_args} \
    {image} \"${{INPUT[$SLURM_ARRAY_TASK_ID]}}\"",
            input_array = command_arg_vec.join("\n"),
            volume_args = volume_args_for_container(args.container),
            podman_args = args.podman_args.unwrap_or("".to_string()),
        )?;
    } else {
        return Err(anyhow!("Unable to take stdin of sbatch"));
    }
    Ok(sbatch.wait()?)
}

fn volume_args_for_container(c: Container) -> &'static str {
    match c {
        Container::Freesurfer => {
            "-v /mnt/apps/etc/fs_license.txt:/usr/local/freesurfer/.license:ro \
-v /opt/matlab/runtime/R2019b/v97/:/usr/local/freesurfer/MCRv97"
        }
        Container::Other => "",
    }
}

fn container_image(c: Container) -> Option<&'static str> {
    match c {
        Container::Freesurfer => Some("freesurfer/freesurfer:7.3.2"),
        Container::Other => None,
    }
}
