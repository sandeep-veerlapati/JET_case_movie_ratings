from setuptools import setup, find_packages

build_variables = {
    "module_name":"JET-movie-ratings",
    "version" : "0.1"
}

setup(name=build_variables["module_name"],
      version=build_variables["version"],
      description="JET Case study",
      author="Sandeep Kumar Veerlapati",
      author_email="sandeeps1244@gmail.com",
      url="",
      package_dir={"": "src"},
      packages=find_packages("src"),
      scripts=["scripts/download_files","scripts/movie_ratings_ingestion.py"]
      )
