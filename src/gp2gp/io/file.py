from pathlib import Path


def write_to_text_file(content: str, file_path: str):
    path = Path(file_path)
    path.write_text(content)
