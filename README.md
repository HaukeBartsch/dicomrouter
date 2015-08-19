DICOM ROUTER
============

DICOM series are still mainly stored as single slices which creates hundereds of images for each of our advanced MRI image series (RSI/DTI/fMRI). This is an attempt to create a fast solution that can distribute DICOM files as fast as they can be received over our network.

The program detects the end of a study and sends out an email.:

    BTUANON TRANSFER SUCCESS
    BTUANON: MMILREC TRANSFER SUCCESS for 1 files (total number of files received: 4707)
    study description: "MRI BRAIN WO/W CONTRAST"
    file destination: "/.../BTUAnon/.../1.2.826.0..."

The file is controlled by routes stored in a separate json file in the local directory:

    [
      { "AETITLE" : "BTUANON",
        "PATH" : "/space/md8/3/data/MMILDB/PLING/orig",
	"EMAIL" : [ "hbartsch@ucsd.edu" ]
      }
    ]

Installation
------------

This program requires python 2.7 and pydicom. Once started with:

     python2.7 processSingleFile.py start

it will daemon-ize itself (work in the background) and wait for DICOM slices it needs to process. The reason why this is nice is that the python script does not need to be restarted for every image that arrives. On our machines this solution can distribute images as fast as they arrive (produces a load of 0.85).

In order to tell the program to work (e.g. look at DICOM tags and decide where to put the slice) write some information into a pipe the programs listens to. Here an example controlled by dcmtk's storescp as DICOM listener:

   pipe=/tmp/.processSingleFilePipe
   /usr/pubsw/packages/dcmtk/3.6.0/bin/storescp --fork \
                                                --write-xfer-little \
                                                --exec-on-reception "echo '#a,#c,#r,#p,#f' >$pipe" \
                                                --sort-on-study-uid scp \
                                                --output-directory "/tmp/archive" \
                                                11113
