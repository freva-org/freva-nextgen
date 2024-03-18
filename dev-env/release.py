"""Script that prepares a new release of a version."""

import argparse
import json
import logging
import re
import tempfile
from functools import cached_property
from itertools import product
from pathlib import Path

import git
import tomli
from packaging.version import Version, InvalidVersion

# Set up logging
logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    datefmt="[%X]",
    level=logging.INFO,
)


logger = logging.getLogger("create-release")


class Exit(Exception):
    """Custom error class for representing specific errors.

    Attributes:
        message (str): A human-readable message describing the error.
        code (int): An error code associated with the error.
    """

    def __init__(self, message: str, code: int = 1) -> None:
        """Initialize the CustomError instance with the given message and error code.

        Args:
            message (str): A human-readable message describing the error.
            code (int): An error code associated with the error.
        """
        super().__init__(message)
        self.message = message
        logger.critical(message)
        raise SystemExit(code)


class Release:
    """Release class."""

    version_pattern: str = r'__version__\s*=\s*["\'](\d+\.\d+\.\d+)["\']'

    def __init__(self, package_name: str, repo_dir: str, branch: str = "main") -> None:
        self.branch = branch
        self.package_name = package_name
        self.repo_dir = Path(repo_dir)
        logger.info("Searching for packages/config with the name: %s", package_name)
        logger.debug("Reading current git config")
        self.git_config = (
            Path(git.Repo(search_parent_directories=True).git_dir) / "config"
        ).read_text()

    def tag_version(self) -> None:
        """Tag the latest git version."""

    @cached_property
    def repo_url(self) -> str:
        """Get the current git repo url."""
        repo = git.Repo(search_parent_directories=True)
        return repo.remotes.origin.url

    @cached_property
    def git_tag(self) -> Version:
        """Get the latest git tag."""
        logger.debug("Searching for the latest tag")
        repo = git.Repo(self.repo_dir)
        try:
            # Get the latest tag on the main branch
            return Version(
                repo.git.describe("--tags", "--abbrev=0", self.branch).lstrip("v")
            )
        except git.exc.GitCommandError:
            logger.debug("No tag found")
        except InvalidVersion:
            logger.debug("Tag found, but could not parse version")
        return Version("0.0.0")

    @property
    def version(self) -> Version:
        """Get the version of the current software."""
        logger.debug("Searching for software version.")
        pck_dirs = Path("src") / self.package_name, Path(
            "src"
        ) / self.package_name.replace("-", "_")
        files = [
            self.repo_dir / f[1] / f[0]
            for f in product(("_version.py", "__init__.py"), pck_dirs)
        ]
        files += [
            self.repo_dir / Path("package.json"),
            self.repo_dir / "pyproject.toml",
        ]
        for file in files:
            if file.is_file():
                if file.suffix == ".py":
                    match = re.search(self.version_pattern, file.read_text())
                    if match:
                        return Version(match.group(1))
                elif file.suffix == ".json":
                    content = json.loads(file.read_text())
                    if "version" in content:
                        return Version(content["version"])
                elif file.suffix == ".toml":
                    content = tomli.loads(file.read_text())
                    if "project" in content:
                        return Version(content["project"]["version"])
        raise ValueError("Could not find version")

    def _clone_repo_from_franch(self, branch: str = "main") -> None:
        logger.debug(
            "Cloning repository from %s with branch %s to %s",
            self.repo_url,
            self.repo_dir,
            branch,
        )
        git.Repo.clone_from(self.repo_url, self.repo_dir, branch=branch)
        (self.repo_dir / ".git" / "config").write_text(self.git_config)

    def _check_change_lock_file(self) -> None:
        """Check if the current version was added to the change lock file."""
        logger.debug("Checking for change log file.")
        if not self._change_lock_file.is_file():
            raise Exit(
                "Could not find change log file. "
                f"Create one first and push it to the {self.branch} branch."
            )
        if f"v{self.version}" not in self._change_lock_file.read_text("utf-8"):
            raise Exit(
                "You need to add the version v{} to the {} change log file "
                "and push the update to the {} branch".format(
                    self.version,
                    self._change_lock_file.relative_to(self.repo_dir),
                    self.branch,
                )
            )

    @cached_property
    def _change_lock_file(self) -> Path:
        """Find the change lock file."""
        for prefix, suffix in product(("changelog", "whats-new"), (".rst", ".md")):
            for search_pattern in (prefix, prefix.upper()):
                glob_pattern = f"{search_pattern}{suffix}"
                logger.debug("Searching for %s", glob_pattern)
                for file in self.repo_dir.rglob(glob_pattern):
                    return file
        return Path(tempfile.mktemp())

    def tag_new_version(self) -> None:
        """Tag a new git version."""
        self._clone_repo_from_franch(self.branch)
        cloned_repo = git.Repo(self.repo_dir)
        if self.version <= self.git_tag:
            raise Exit(
                "Tag version: {} is the same as current version {}"
                ", you need to bump the verion number first and "
                "push the changes to the {} branch".format(
                    self.version,
                    self.git_tag,
                    self.branch,
                )
            )
        self._check_change_lock_file()
        logger.info("Creating tag for version v%s", self.version)
        head = cloned_repo.head.reference
        message = f"Create a release for v{self.version}"
        try:
            cloned_repo.create_tag(f"v{self.version}", ref=head, message=message)
            cloned_repo.git.push("--tags")
        except git.GitCommandError as error:
            raise Exit("Could not create tag: {}".format(error))
        logger.info("Tags created.")

    @classmethod
    def cli(cls, temp_dir: str) -> "Release":
        """Command line interface."""

        parser = argparse.ArgumentParser(
            description="Prepare the release of a package."
        )
        subparser = parser.add_subparsers(help="Available commands:")
        tag_parser = subparser.add_parser("tag", help="Create a new tag")
        deploy_parser = subparser.add_parser(
            "deploy", help="Update the version in the deployment repository"
        )
        for _parser in tag_parser, deploy_parser:
            _parser.add_argument("name", help="The name of the software/package.")
            _parser.add_argument(
                "-v",
                "--verbose",
                help="Enable debug mode.",
                action="store_true",
            )
        tag_parser.add_argument(
            "-b",
            "--branch",
            help="Set the working branch",
            type=str,
            default="main",
        )
        args = parser.parse_args()
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        return cls(args.name, temp_dir, args.branch)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temporary_dir:
        try:
            release = Release.cli(temporary_dir)
            release.tag_new_version()
        except Exception as error:
            if logger.getEffectiveLevel() == logging.DEBUG:
                raise
            raise Exit("An error occurred: {}".format(error)) from error
