#!/usr/bin/env python

import argparse

def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("samples", type=str)
    ap.add_argument("taxonomies", type=str)
    ap.add_argument("webin_credentials", type=str)
    ap.add_argument("--workdir", "-w", type=str, default="work")
    
    args = ap.parse_args()


if __name__ == "__main__":
    main()