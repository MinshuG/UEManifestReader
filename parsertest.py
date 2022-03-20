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
getLogger("UE4Parse").setLevel(level=logging.INFO)

def main():
    base = "https://epicgames-download1.akamaized.net/Builds/Fortnite/CloudDir/"
    manifest_data = asyncio.run(UEManifestReader.UEManifestReader().download_manifest(return_parsed = False))
    manifest = FManifestData(manifest_data)

    base = base + manifest.Meta.ChunkSubDir + "/"

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
            # continue
            manifest_stream = ManifestFileStream(manifest, file, base)
            utoc_stream = BinaryStream(manifest_stream, manifest_stream.size)

            ucas_name = file.Filename.replace(".utoc", ".ucas")
            for f in manifest.FileManifestList.FileManifest: # find .ucas
                if f.Filename == ucas_name:
                    manifest_stream = ManifestFileStream(manifest, f, base)
                    ucas_stream = BinaryStream(manifest_stream, manifest_stream.size)
                    provider.initialize(file.Filename, (utoc_stream, ucas_stream))
                    break

    provider.submit_key(FGuid(0,0,0,0), FAESKey("0xB30A5DBC657A27FBC9E915AFBFBB13F97A3164034F32B1899DEA714CD979E8C3"))
    for k, v in provider.files:
        print(v.Name)
        reader = v.get_data()
        # print(reader.read())
        # break
        # pkg = provider.try_load_package(k)
        # print(json.dumps(pkg.get_dict(), indent=4))
        # break
main()