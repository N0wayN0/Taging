# Taging
Taging System

Usage:
    
##Add Update and rewrite tags

    Write new sets of tags;
    $ update /path/to/file tag1 tag2 tag3 "tag 4"

    Same Spare Option:
    $ insert /path/to/file tag1 tag2 "tag 3" "tag 4"
    
    Add new tags to existing file:
    $ add /path/to/file new_tags
    
    
##Search

    Show all files with given tags:
    $ search tag1 AND "tag 2" OR tag3
    
    Search for 'word' in path name:
    $ in_path word 

    Get all tags of given file:
    $ get_tags /path/to/file


##Maintain

    Shows wchich files are taged in given directory:
    $ stat_dir /path/to/dir
    
    If file was moved this will update its path:
    $ update_path /path/to/file

    Remove tags of given file:
    $ remove /path/to/file
    
    Shows all records in database:
    $ show_all spare
