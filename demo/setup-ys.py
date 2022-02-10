# from distutils.core import setup
from setuptools import setup, find_packages
import npsdkk

with open("README.md", mode="r", encoding='utf-8') as fh:
    long_description = fh.read()
    
setup(
    name='npsdkk',
    description="Nezip Python SDK",
    version='0.9.6',
    author='xxg',
    author_email='752299578@qq.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    # packages=find_packages(include=['npsdkk', 'npsdkapi.py', 'npsdkobjs.py', 'Stockdrv.py']),
    packages=find_packages(),
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Communications', 'Topic :: Internet',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ],
    zip_safe=True,
    python_requires='>=3.6',
    
)