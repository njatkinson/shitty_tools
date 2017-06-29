import os
import tempfile
from collections import MutableMapping


class FileSystemDict(MutableMapping):
    def __init__(self, storage_path, scratch_path):
        '''
        :param storage_path: directory for storing data
        :param scratch_path: directory for staging writes
        '''
        self.storage_path = os.path.abspath(storage_path)
        self.scratch_path = os.path.abspath(scratch_path)

        if not os.stat(self.storage_path).st_dev == os.stat(self.scratch_path).st_dev:
            # This will pass even if scratch and storage are on different devices in Windows
            # but that's okay, because Windows doesn't atomic move even within the same fs
            raise Exception('Storage path and scratch path must be on the same file system')
        if os.path.split(self.scratch_path)[0].startswith(self.storage_path):
            raise Exception('Scratch path must not be accessible from storage path')


    def _get_storage_key_path(self, key):
        key_path = os.path.abspath(os.path.join(self.storage_path, str(key)))
        if not key_path.startswith(self.storage_path):
            raise Exception('Attempted file system traversal with key: %s' %key)
        return key_path


    def __getitem__(self, key):
        try:
            with open(self._get_storage_key_path(key), 'r') as infile:
                return infile.read()
        except IOError:
            raise KeyError(key)


    def __setitem__(self, key, value):
        destination_path = self._get_storage_key_path(key)
        scratch = tempfile.NamedTemporaryFile(mode='w', dir=self.scratch_path, delete=False)
        try:
            scratch.write(value)
        except:
            scratch.close()
            os.remove(scratch.name)
            raise
        scratch.close()
        try:
            os.renames(scratch.name, destination_path)
        except OSError as e:
            try:
                # On Windows you can't overwrite via rename. See if that's the problem.
                os.remove(destination_path)
                os.renames(scratch.name, destination_path)
            except:
                # If we've got to this point something has gone wrong. Most likely user is trying
                # to create a subdirectory with the same name as a file.
                os.remove(scratch.name)
                raise


    def __delitem__(self, key):
        try:
            os.remove(self._get_storage_key_path(key))
        except OSError:
            # Non-existant file
            raise KeyError(key)


    def __iter__(self):
        for dirpath, dirnames, filenames in os.walk(self.storage_path):
            for filename in filenames:
                yield os.path.join(dirpath, filename)


    def __len__(self):
        return sum([len(filenames) for (dirpath, dirnames, filenames) in os.walk(self.storage_path)])