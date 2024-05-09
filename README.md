# pypipe
Automated data evaluation framework.

Also, maybe will be also misused as a repository of other less related Python goodies.

See also dvc.org

# Ideas

Extend an file system to allow "promise" or "built" targets.
These are "empty" files with annotation of how to obtain them, i.e., a generating call.
That call may be quite complex.
The filesystem needs to distinguish the mode of opening the file - either you edit the file itself, or the generating rule.
Thus, one could be able to implement
- ordinary "primary" file storage
- cached files
- built files

Then, each file could have also a "deletor" script, which would, e.g.,
- move it to trash
- delete permanently (default)
- archive it by some rule
