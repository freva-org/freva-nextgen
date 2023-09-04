"""Read the version of the installed package from the package resources."""
import pkg_resources

dist = pkg_resources.get_distribution("databrowser")
__version__ = dist.version
