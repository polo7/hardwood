#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import logging
import subprocess

log = logging.getLogger("mkdocs.hooks.git_commit")


def on_config(config):
    if not config["extra"].get("commit_sha"):
        try:
            subprocess.run(
                ["git", "config", "--global", "--add", "safe.directory", "/repo"],
                check=False,
            )
            sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.STDOUT
            ).strip()
            config["extra"]["commit_sha"] = sha
        except Exception as e:
            log.warning("Could not determine git commit SHA: %s", e)
    return config
