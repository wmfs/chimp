from distutils.core import setup

setup(
    name='Chimp',
    version='1.0.0',
    author='Tim Needham',
    author_email='tim.needham@wmfs.net',
    packages=['chimp','chimp.alert','chimp.build','chimp.calc','chimp.extract','chimp.load','chimp.spec','chimp.taskqueue'],
    url='https://github.com/wmfs/chimp',
    license='LICENSE.txt',
    description='A dynamic PostgreSQL database generator, and integrated extract, transform and load tool.',
    long_description=open('README.txt').read(),
    install_requires=[
        "psycopg2 >= 2.4.5",
    ],
)
