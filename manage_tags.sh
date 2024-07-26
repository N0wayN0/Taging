#!/bin/bash

tag=$(tagi.py status 2>/dev/null | fzf)
echo $tag
tagi.py search "$tag"| sxiv -
