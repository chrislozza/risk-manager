use anyhow::bail;
use anyhow::Result;
use nix::unistd::fork;
use nix::unistd::ForkResult;
use std::io;
use std::io::BufRead;
use std::process::Command;
use std::process::Stdio;
use tracing::error;
use tracing::info;

pub struct ExternalProcess {}

impl ExternalProcess {
    pub fn launch_cloud_proxy(&self) -> Result<()> {
        info!("Chris in here");
        let process_name = "cloud-sql-proxy";
        let args = vec![
            "savvy-nimbus-306111:europe-west2:vibgo-sql",
            "-p=5433",
            "-c=/root/.ssh/service-client.json",
        ];
        match self.launch_process(process_name, args) {
            io::Result::Err(err) => {
                bail!("{}", err)
            }
            _ => Ok(()),
        }
    }

    fn launch_process(&self, process_name: &str, args: Vec<&str>) -> io::Result<()> {
        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                // This is the parent process
                info!("Parent process (PID: {})", nix::unistd::getpid());

                // Wait for the child process to finish
                let status = nix::sys::wait::waitpid(child, None)?;
                info!("Child process (PID: {}) exited with: {:?}", child, status);
            }
            Ok(ForkResult::Child) => {
                // This is the child process
                println!("Child process (PID: {})", nix::unistd::getpid());

                let mut child_process = Command::new(process_name)
                    .args(args)
                    .stdout(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()?;

                // Create reader instances for stdout and stderr of the child process
                let stdout = child_process
                    .stdout
                    .take()
                    .expect("Failed to capture stdout");
                let stderr = child_process
                    .stderr
                    .take()
                    .expect("Failed to capture stderr");

                // Spawn Tokio tasks to read and print stdout and stderr concurrently
                tokio::spawn(Self::read_and_print_stream("stdout", stdout));
                tokio::spawn(Self::read_and_print_stream("stderr", stderr));

                // Wait for the child process to finish
                let status = child_process.wait()?;
                println!("Child process exited with: {:?}", status);

                // Exit the child process
                std::process::exit(0);
            }
            Err(_) => eprintln!("Fork failed"),
        }

        Ok(())
    }

    async fn read_and_print_stream<Out>(name: &str, stream: Out)
    where
        Out: std::io::Read,
    {
        let reader = io::BufReader::new(stream);
        let lines = reader.lines();
        for line in lines {
            match name {
                "stdout" => info!("{}: {}", name, line.unwrap()),
                "stderr" => error!("{}: {}", name, line.unwrap()),
                _ => panic!("Shouldn't have got here"),
            }
        }
    }
}
