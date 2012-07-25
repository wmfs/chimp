Chimp
v 1.0

written by Tim Needham tim.needham@wmfs.net

A dynamic gazetter generator, ETL... *mumble mumble*... holistic

(c) West Midlands Fire Service 2011
Licensed under the GNU General Public License v3. See LICENCE.txt for details.


INSTALL:
--------

1) extract the distribution file using 'tar zxfv chimp-1.0.tar.gz'
2) cd into the chimp-1.0 directory
3) download the repository archive
4) extract the repository distribution using 'tar zxfv chimp_repository-1.0.tar.gz'
5) create a local settings.xml file by copying chimp-1.0/config/settings.xml.default and editing appropriately

USAGE:
------


Building

cd chimp-1.0
python3 chimp/chimp.py build --specification <spec_name>


Importing

cd chimp-1.0
python3 chimp/chimp.py import ...

Queue

cd chimp-1.0
python3 chimp/chimp.py queue ...

