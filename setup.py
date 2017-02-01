#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name="toil-signalAlign",
      version="0.1.0",
      description="Toil script for signal-level analysis of ONT data",
      author="Art Rand",
      author_email="arand@soe.ucsc.edu",
      url="https://github.com/ArtRand/toil-signalAlign",
      package_dir={"": "src"},
      packages=find_packages("src"),
      install_requires=["signalAlign>=0.1.1",
                        "marginAlign==1.1.7",
                        "toil-lib==1.2.0a1.dev139"],
      entry_points={
          "console_scripts": ["toil-signalAlign = toil_signalalign.toil_signalalign_pipeline:main"]})
