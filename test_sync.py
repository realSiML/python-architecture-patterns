from pathlib import Path
import shutil
import tempfile

from sync import sync


class FakeFileSystem(list):
    def copy(self, src, dest):
        self.append(("COPY", src, dest))

    def move(self, src, dest):
        self.append(("MOVE", src, dest))

    def delete(self, dest):
        self.append(("DELETE", src, dest))


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    source = {"sha1": "my-file"}
    dest = {}
    filesystem = FakeFileSystem()
    reader = {"/source": source, "/dest": dest}
    sync(reader.pop, filesystem, "/source", "/dest")
    assert filesystem == [("COPY", Path("/source/my-file"), Path("/dest/my-file"))]


def test_when_a_file_has_been_renamed_in_the_source():
    source = {"sha1": "renamed-file"}
    dest = {"sha1": "original-file"}
    filesystem = FakeFileSystem()
    reader = {"/source": source, "/dest": dest}
    sync(reader.pop, filesystem, "/source", "/dest")
    assert filesystem == [
        ("MOVE", Path("/dest/original-file"), Path("/dest/renamed-file"))
    ]
