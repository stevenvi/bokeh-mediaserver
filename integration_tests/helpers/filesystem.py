import os


def item_data_path(data_path: str, file_hash: str) -> str:
    """
    Return the base directory for a media item's derived data.

    Mirrors imaging.ItemDataPath: three-level hex sharding from BLAKE2b-256 hash.
        {data_path}/{hash[0:2]}/{hash[2:4]}/{hash[4:]}/
    """
    return os.path.join(data_path, file_hash[0:2], file_hash[2:4], file_hash[4:])


def variant_path(data_path: str, file_hash: str, variant: str, fmt: str = "avif") -> str:
    """Return the expected filesystem path for a named image variant."""
    return os.path.join(item_data_path(data_path, file_hash), f"{variant}.{fmt}")


def dzi_manifest_path(data_path: str, file_hash: str) -> str:
    """Return the path to the DZI manifest file."""
    return os.path.join(item_data_path(data_path, file_hash), "tiles", "image.dzi")


def dzi_tiles_dir(data_path: str, file_hash: str) -> str:
    """Return the directory that contains DZI tile level subdirectories."""
    return os.path.join(item_data_path(data_path, file_hash), "tiles", "image_files")
