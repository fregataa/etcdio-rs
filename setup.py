from setuptools import setup
from setuptools_rust import Binding, RustExtension

setup(
    name="etcdio-rs",
    version="0.1",
    rust_extensions=[RustExtension("etcdio_rs.etcdio_rs", binding=Binding.PyO3)],
    packages=["etcdio_rs"],
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
)
