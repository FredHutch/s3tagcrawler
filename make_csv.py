#!/usr/bin/env python3

"""
Convert partial csv to full csv
"""

import csv
import sys



def main():
    "do the work"
    if len(sys.argv) != 4:
        print("Usage: {} swiftlistingfile partialcsvfile.csv  outputfile.csv".format(sys.argv[0]))
        sys.exit(1)
    swiftlistingfile = sys.argv[1]
    partialcsvfile = sys.argv[2]
    outputfile = sys.argv[3]
    csvrows = []
    with open(partialcsvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            csvrows.append(row)
    swiftlisting = []
    with open(swiftlistingfile) as swiftfile:
        swiftlisting = swiftfile.readlines()
    swiftlisting = [x.strip() for x in swiftlisting]


    outkeys = reader.fieldnames #list(csvrows[0].keys())
    outkeys.remove('filename_string')
    otherkeys = outkeys.copy()
    otherkeys.remove('seq_dir')


    with open(outputfile, 'w') as outcsv:
        writer = csv.DictWriter(outcsv, fieldnames=outkeys)
        writer.writeheader()
        for row in csvrows:
            row = dict(row)
            prefix = row['seq_dir'].replace("swift://", "")
            filename_string = row['filename_string']
            prefix = "{}/{}".format(prefix, filename_string)
            matches = [x for x in swiftlisting if x.startswith(prefix)]
            for match in matches:
                outdict = dict(seq_dir=match)
                for item in otherkeys:
                    outdict[item] = row[item]
                # print(outdict)
                writer.writerow(outdict)



if __name__ == "__main__":
    main()
