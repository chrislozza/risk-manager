import os
import sys
import argparse
import subprocess
from subprocess import PIPE

from utils.logger import l_info, l_init, l_exception

import utils.docker_admin as docker_admin

def run_docker_build(args, image_root):
    command_name = ["docker", "build", "--build-arg", f"key={args.key}", "--build-arg", f"secret={args.secret}", "--build-arg", f"settings={args.settings}", "--build-arg", f"account={args.account}", "-t", f"{args.name}:{args.tag}", f"{image_root}"]
    for path in docker_admin.run_subprocess_cmd(command_name):
        print(path)

def main():
    l_init("INFO")
    parser = argparse.ArgumentParser(prog="a risk management tool", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--name", required=True, default="trading-app", help="name of image")
    parser.add_argument("--tag", required=True, help="image tag")
    parser.add_argument("--settings", required=True, help="trading app settings file [json]")
    parser.add_argument("--service-key", required=True, help="service-key json file for google credentials")
    parser.add_argument("--key", required=True, help="broker api key")
    parser.add_argument("--secret", required=True, help="broker api secret")
    parser.add_argument("--account", default="paper", help="broker account type [paper|live]")
    args = parser.parse_args()

    try:
        root = os.getcwd()
        image_root = f"{root}/docker"

        build_type = "debug" if args.account == "paper" else "release"
        docker_admin.copy_files(
            src_files=[
                f"{args.settings}",
                f"{args.service_key}",
                f"target/{build_type}/trading-app",
            ],
            tgt_path=f"{image_root}/config",
        )
        docker_admin.rename_files(
            files={
                args.settings: "settings.json",
                args.service_key: "service_client.json",
            },
            tgt_path=f"{image_root}/config",
        )
        run_docker_build(args, image_root)
        docker_admin.clean_up(
            file_clean=[
                f"{image_root}/config/settings.json",
                f"{image_root}/config/trading-app",
                f"{image_root}/config/service_client.json",
            ]
        )

    except Exception as e:
        l_exception(f"Issue detected in build {e}")


if __name__ == "__main__":
    main()
