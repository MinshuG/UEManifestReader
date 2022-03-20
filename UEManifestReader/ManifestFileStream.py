import chunk
from typing import TYPE_CHECKING, List, Tuple
import requests

from UEManifestReader.classes.FFileManifestList import FChunkPart

if TYPE_CHECKING:
    from .classes import FManifestData
    from .classes.FFileManifestList import FFileManifest

class ManifestFileStream:
    _manifest: "FManifestData"
    FileName: str
    Size: int
    _chunk_parts: List["FChunkPart"]
    _position: int = 0

    @property
    def size(self):
        return self.Size

    def __init__(self, manifest: "FManifestData", file: "FFileManifest", base_url: str):
        self.base_url = base_url
        self.client = requests.Session()
        self._manifest = manifest
        self.FileName = file.Filename
        self._chunk_parts = file.ChunkParts

        self.Size = 0
        for c in self._chunk_parts:
            self.Size += c.Size

        self.__previous_chunk = None

    def read(self, size=None):
        if size is None:
            size = self.Size - self._position  # till end
        buffer = b""
        client = self.client

        read_count = 0
        _, start_pos, chunk_index = self.GetChunkIndex(self._position)
        while True:
            chunk_part = self._chunk_parts[chunk_index]
            chunk = self._manifest.ChunkDataList.get_by_guid(chunk_part.Guid)

            chunk_offset = chunk_part.Offset + start_pos
            stream = chunk.GetStream(chunk_offset, client, self.base_url)

            chunkBytes = chunk_part.Size - start_pos
            bytesLeft = size - read_count

            if (bytesLeft <= chunkBytes):
                buffer += stream.read(bytesLeft)
                read_count += bytesLeft
                # stream.close()
                break

            buffer += stream.read(chunkBytes)
            # stream.close()
            read_count += chunkBytes

            start_pos = 0
            chunk_index += 1
            if chunk_index >= self._chunk_parts.__len__():
                break

        self._position += read_count
        return buffer

    def GetChunkIndex(self, position) -> Tuple['FChunkPart', int, int]:
        """returns Chunk and Position in chunk"""
        if self.__previous_chunk and position < self.__previous_chunk[0].Size:
            return self.__previous_chunk[0], position, self.__previous_chunk[1]

        for i in range(self._chunk_parts.__len__()):
            c = self._chunk_parts[i]
            size = c.Size
            if position < size:
                self.__previous_chunk = (c, i)
                return c, position, i
            position -= size
        raise ValueError("Requested chunk not found")

    def close(self):
        del self._chunk_parts
        self._chunk_parts = None
        self.client.close()

    def tell(self):
        return self._position

    def seek(self, offset: int, whence: int):
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        elif whence == 2:
            self._position = self.Size - offset  # ??
        else:
            raise ValueError("invalid SEEK_SET")
