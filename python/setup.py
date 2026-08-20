from __future__ import annotations

import os
from pathlib import Path

from setuptools import setup
from wheel.bdist_wheel import bdist_wheel


class PlatformWheel(bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        bundled = list((Path(__file__).parent / "src" / "sharkbite" / ".libs").glob("*"))
        self._has_bundled_native = any(
            path.suffix.lower() in {".dll", ".dylib", ".so"} for path in bundled
        )
        self.root_is_pure = not self._has_bundled_native

    def get_tag(self) -> tuple[str, str, str]:
        if not self._has_bundled_native:
            return "py3", "none", "any"
        _, _, default_platform = super().get_tag()
        platform_tag = os.environ.get("SHOAL_WHEEL_PLATFORM_TAG", default_platform)
        return "py3", "none", platform_tag


setup(cmdclass={"bdist_wheel": PlatformWheel})
