import asyncio
import json
from logging import getLogger
import logging
import os
from UE4Parse.Provider.StreamedFileProvider import StreamedFileProvider
from UE4Parse.BinaryReader import BinaryStream
from UE4Parse.Assets.Objects.FGuid import FGuid
from UE4Parse.Encryption.FAESKey import FAESKey
import UEManifestReader
from UEManifestReader.ManifestFileStream import ManifestFileStream
from UEManifestReader.classes.FManifestData import FManifestData

getLogger("ManifestDownloader").setLevel(level=logging.DEBUG)
getLogger("UE4Parse").setLevel(level=logging.DEBUG)

def main():
    base = "https://epicgames-download1.akamaized.net/Builds/Fortnite/CloudDir/"
    manifest_data = asyncio.run(UEManifestReader.UEManifestReader().download_manifest(return_parsed = False))
    manifest = FManifestData(manifest_data)

    base = base + manifest.Meta.ChunkSubDir + "/"
    manifest.base_url = base
    provider = StreamedFileProvider()

    for file in manifest.FileManifestList.FileManifest:
        if not file.Filename.startswith("FortniteGame/Content/Paks/"):
            continue
        if file.Filename.endswith(".pak"):
            manifest_stream = ManifestFileStream(manifest, file, base)
            stream = BinaryStream(manifest_stream, manifest_stream.size)
            # os.makedirs(os.path.dirname(file.Filename), exist_ok=True)
            # with open(file.Filename, "wb") as f:
            #     f.write(stream.read())
            provider.initialize(file.Filename, (stream,))
        elif file.Filename.endswith(".utoc"):
            manifest_stream = ManifestFileStream(manifest, file, base)
            utoc_stream = BinaryStream(manifest_stream, manifest_stream.size)
            provider.initialize(file.Filename, (utoc_stream, manifest.get_file_stream))

    provider.submit_key(FGuid(0,0,0,0), FAESKey("0x53839BA2A77AE393588184ACBD18EDBC935CA60D554F9D29BC3F135E426C4A6F"))
    for k, v in provider.files:
        print(v.Name)
        reader = v.get_data()
        print(reader.read())
        break
    #     # pkg = provider.try_load_package(k)
    #     # print(json.dumps(pkg.get_dict(), indent=4))
    #     # break
main()