
import setuptools

setuptools.setup(
    name='cap2',
    version='0.4.1',
    description="CAP2",
    author="David C. Danko",
    author_email='dcdanko@gmail.com',
    url='https://github.com/metasub/cap2',
    packages=setuptools.find_packages(),
    package_dir={'cap2': 'cap2'},
    install_requires=[
        'pandas==1.3.4,<2.0',
        'numpy',
        'scipy==1.7.3,<2.0',
        'python-daemon>=2.3.0,<3.0',
        'luigi>=3.0.3,<3.1',
        'Pillow==9.0.0',
        'PyYaml==6.0',
        'biopython==1.76',
        'click>=6.0',
        'Jinja2==3.0.3',  # for multiqc
        'multiqc==1.11',  # hackish, tbd if I'm okay with this
        'pangea_api>=0.9.24,<1.0',
        'pysam==0.18.0',
        'python-louvain==0.15',
        'gimmebio.seqs==0.9.3',
        'bloom_filter>=1.3.3,<2.0',
    ],
    entry_points={
        'console_scripts': [
            'cap2=cap2.cli:main',
        ]
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
)
