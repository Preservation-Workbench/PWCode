# Microgit
Git for Micro

## About
Adds a number of commands to make working with git repos within Micro a more seamless experience. Additionally, adds statusline parameters to make showing branch information on your statusline easily configurable. Finally, improves on the diff plugins git change support.

## Installation
1. Navigate to your micro configuration plug directory:
  ```
  cd /path/to/micro/dir/plug
  ```
2. Clone the repo as `microgit`
  ```
  git clone https://github.com/SleepyFugu/microgit-plugin microgit
  ```

## Building
- This project uses [moonscript](https://moonscript.org/), so if you're making changes you'll need to install that via [luarocks](https://luarocks.org/):
  ```
  luarocks install moonscript
  ```
- Following that, just run `moonc` in the plugin directory
  ```
  cd /path/to/microgit && moonc .
  ```

## Options
- `microgit.updateinfo (boolean)` 
  - Enable/Disable branch information tracking in Micro
  - Note: This is required for the statusline functions to work properly
- `microgit.command (string)`
  - Path to the git command that you would like to use
- `microgit.gitgutter (boolean)`
  - Enable/Disable diff base updates during saving, buffer open, and command run
  - To use properly, enable `diffgutter`

## Statusline
NOTE: Please note that the "microgit" within the following functions will correspond to what you have named the plugin within your plug directory.
This plugin exposes the following functions for statusline configurations:
  - `microgit.numahead`
    - Returns the number of commits ahead of your branches origin
  - `microgit.numbehind`
    - Returns the number of commits behind of your branches origin
  - `microgit.numstaged`
    - Returns the number of staged files in your branch
  - `microgit.oncommit`
    - Returns the current commit short hash
  - `microgit.onbranch`
    - Returns the current panes branch (if any)
  - Example:
    ```
    $(microgit.onbranch) ↑$(microgit.numahead) ↓$(microgit.numbehind) ↓↑$(microgit.numstaged) | commit:$(microgit.oncommit)
    ```

## Commands
  - `git.init()`
    - Initialize repository in current directory
  - `git.pull()`
    - Pulls changes from remote into the local tree
  - `git.push(str)`
    - Push changes from local onto remove for the provided label (likely a branch)
  - `git.list()`
    - List the local and remote branches
  - `git.log()`
    - Show the commit log
  - `git.checkout(str)`
    - Checkout a specific label (tag/branch/revision)
  - `git.commit(str)`
    - Commit changes to your local tree. If `str` is provided, it is used as the commit message. Otherwise, a buffer is opened for editting and the contents will be used instead
  - `git.commit(str)`
    - Checkout a new branch, and switch to it
  - `git.status()`
    - Show current status of the repository
  - `git.stage(str...)`
    - Stage one or more files. If `--all` is provided, stage all files
  - `git.unstage(str...)`
    - Unstage one or more files. If `--all` is provided, unstage all files
  - `git.rm`
    - Remove one or more files. If `.` is provided, remove all files


## Environment
This plugin comes with a debug command not listed above called (`git.debug`), to enable this command set `MICROGIT_DEBUG=1` in your environment
