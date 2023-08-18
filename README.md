# Taging
Taging System

Usage:
    
    Add Update and rewrite tags
    $ update /path/to/file tag1 tag2 tag3 "tag 4"

    $ insert /path/to/file tag1 tag2 "tag 3" "tag 4"
    
    $ add /path/to/file new_tags
    
    
    Search
    $ search tag1 AND "tag 2" OR tag3
    
    $ in_path word "search for world in path name"
    
    $ get_tags /path/to/file

    Maintain:
    $ stat_dir /path/to/dir
    
    $ update_path /path/to/file
                  
    $ remove /path/to/file
                  
    $ show_all spare
