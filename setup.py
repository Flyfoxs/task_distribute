#!/usr/bin/env python
from setuptools import setup
import setuptools


with open('README.md') as file:
    long_description = file.read()

setup(
    name='task_distribute',
    version='0.1.9',
    description='Simple way to distribute task to diff server',
    long_description='Simple way to distribute task to diff server',
    url='https://github.com/Flyfoxs/task_distribute.git',
    author='Felix Li',
    author_email='lilao@163.com',
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords='task_distribute',
    packages=setuptools.find_packages(),
)


#pip install git+https://github.com/Flyfoxs/task_distribute@master