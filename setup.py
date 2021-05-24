from setuptools import setup, find_packages

setup(name="pyasrank",
      description="A Python interface to CAIDA ASRank service",
      version="0.1.1",
      author="Mingwei Zhang",
      author_email="mingwei@hotpotato.dev",
      url="http://github.com/digizeph/pyasrank",
      license="MIT",
      keywords='caida asrank',
      packages=find_packages(),
      install_requires=[
          "requests",
          ]
      )