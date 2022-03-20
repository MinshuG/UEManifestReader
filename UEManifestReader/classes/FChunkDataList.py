# -*- coding: utf-8 -*-
from io import BytesIO
import os
import struct
from typing import List
from UEManifestReader import Logger
from UEManifestReader.enums import *
from UEManifestReader.converter import *
from UEManifestReader.classes.stream_reader import ConstBitStreamWrapper

logger = Logger.get_logger(__name__)

class FChunkInfo():
    def __init__(self):
       # The GUID for this data.
        self.Guid = None
        # The FRollingHash hashed value for this chunk data.
        self.Hash = None
        # The FSHA hashed value for this chunk data.
        self.ShaHash = None
        # The group number this chunk divides into.
        self.GroupNumber = None
        # The window size for this chunk
        self.WindowSize = 1048576
        # The file download size for this chunk.
        self.FileSize = None

    @property
    def FileName(self):
        return f"{self.Hash}_{self.Guid}.chunk"

    @property
    def chunkPath(self):
        return f"temp/{self.Hash}_{self.Guid}.chunk"

    def DownloadLink(self, baseurl): # new Uri(baseUri, $"{dataGroup:D2}/{Filename}");
        return f"{baseurl}/{str(self.GroupNumber).zfill(2)}/{self.FileName}"

    def DownloadChunk(self, client, baseurl):
        path = self.chunkPath
        if not os.path.exists(path):  # TODO: check if chunk is corrupted or not
            os.makedirs(os.path.dirname(path), exist_ok=True)
            logger.debug(f"Downloading chunk {self.FileName}")
            chunkSR = client.get(self.DownloadLink(baseurl))
            if chunkSR.status_code == 200:
                io = BytesIO(chunkSR.content)

                io.seek(8)
                header_size = struct.unpack("I", io.read(4))[0]
                io.seek(40)
                is_compressed = int.from_bytes(io.read(1), "little") == 1
                io.seek(header_size)

                if is_compressed:
                    from zlib import decompress
                    decomp_buffer = decompress(io.read())
                    io.close()
                    io = BytesIO(decomp_buffer)
                else:
                    temp_io = BytesIO(io.read())
                    io.close()
                    io = temp_io

                seek_pos = io.tell()
                with open(self.chunkPath, "wb") as f:
                    logger.debug(f"Writing {self.chunkPath}")
                    f.write(io.read())
                io.seek(seek_pos)

                return io, io.getbuffer().nbytes
            else:
                raise Exception("failed to download chunk %d" % self.FileName)

            # logger.info(f"Downloading chunk {self.Id}")
            # chunkSR = requests.get(self.Url)
            # if chunkSR.status_code == 200:
            #         bytes_ = chunkSR.content
            #         io = BytesIO(bytes_)
            #         with open(self.chunkPath, "wb") as f:
            #             logger.debug(f"Writing {self.chunkPath}")
            #             f.write(io.read())
            #         io.seek(0, 0)
            #         return io, len(bytes_)
            # else:
            #     raise Exception("failed to download chunk %d" % self.Id)
        return open(self.chunkPath, "rb"), os.stat(self.chunkPath).st_size

    def GetStream(self, pos, client, baseurl): # relativePos
        stream, size = self.DownloadChunk(client, baseurl)
        stream.seek(pos)
        return stream

class FChunkDataList():
    ChunkList: List[FChunkInfo]

    def __init__(self, reader: ConstBitStreamWrapper):
        StartPos = reader.bytepos
        DataSize = reader.read_uint32()
        DataVersion = reader.read_uint8()

        ElementCount = reader.read_int32()
        self.ChunkList = [FChunkInfo() for _ in range(ElementCount)]
        # For a struct list type of data, we serialise every variable as it's own flat list.
        # This makes it very simple to handle or skip, extra variables added to the struct later.

        # Serialise the ManifestMetaVersion::Original version variables.
        self.ChunkListGuidMap = {}
        if (DataVersion >= EChunkDataListVersion.Original.value):
            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].Guid = self.ReadFChunkInfoGuid(reader)
                self.ChunkListGuidMap[self.ChunkList[idx].Guid] = idx

            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].Hash = ULongToHexHash(reader.read_uint64())

            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].ShaHash = reader.read_bytes(20)

            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].GroupNumber = int(reader.read_uint8())

            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].WindowSize = reader.read_int32()

            for idx, _ in enumerate(self.ChunkList):
                self.ChunkList[idx].FileSize = int(reader.read_uint8())

        # We must always make sure to seek the archive to the correct end location.
        reader.bytepos = StartPos + DataSize

    def ReadFChunkInfoGuid(self, reader: ConstBitStreamWrapper) -> str:
        hex_str = ''
        hex_str += SwapOrder(reader.read_bytes(4))
        hex_str += SwapOrder(reader.read_bytes(4))
        hex_str += SwapOrder(reader.read_bytes(4))
        hex_str += SwapOrder(reader.read_bytes(4))
        return hex_str.upper()

    def get_by_guid(self, guid: str) -> FChunkInfo:
        try:
            return self.ChunkList[self.ChunkListGuidMap[guid]]
        except KeyError:
            raise Exception(f"Chunk with guid {guid} not found")
