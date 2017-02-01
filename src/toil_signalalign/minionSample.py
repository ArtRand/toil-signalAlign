from collections import namedtuple

ReadstoreSample = namedtuple("ReadstoreSample", ["file_type",     # tar or gz-tar
                                                 "URL",           # place to get it
                                                 "size",          # size of tarball, approx
                                                 "sample_label",  # label, appended to ledger
                                                 ])

SignalAlignSample = namedtuple("SignalAlignSample", ["URL",           # place to get BAM
                                                     "size",          # size of BAM, approx
                                                     "sample_label",  # label
                                                     ])
