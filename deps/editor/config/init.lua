VERSION = "1.0.0"

local micro = import("micro")
local config = import("micro/config")
local buffer = import("micro/buffer")
local shell = import("micro/shell")

local function log(args)
  -- for debugging; use micro -debug, and then inspect log.txt
  local info = args.info
  local id = args.id or 'init.lua'

  micro.Log(('📝>> %s: %s'):format(id, info))
end

-- TODO: Lag dummy bindings for shortcuts som ike lar seg bruke så de fortsatt kan være i bindings-fil

-- actions

function VSplitLeft(bp)
  -- Open a new Vsplit (on the very left)
  micro.CurPane():VSplitIndex(buffer.NewBuffer('', ''), false) -- right=false
  micro.InfoBar():Message('New open file')

  log {id='VsplitLeft', info='All done!'}
end

function HSplitUp(bp)
  -- Open a new Hsplit (on the very left)
  micro.CurPane():HSplitIndex(buffer.NewBuffer('', ''), false) -- bottom=false
  micro.InfoBar():Message('New open file')

  log {id='HSplitUp', info='All done!'}
end

function NewView(bp)
  -- Open same file Vsplit (on the very right)
  micro.CurPane():VSplitIndex(bp.Buf, true)  -- right=true
  micro.InfoBar():Message('New View same file')

  log {id='NewView', info='All done!'}
end

function onSave(bp)
  local msg = ('🎬 Saved %s'):format(bp.Buf.Path)
  micro.InfoBar():Message(msg)
  return true
end

-- function init()
   -- -- bindings
-- 
  -- config.TryBindKey(
    -- '\u003cCtrl-n\u003e\u003cCtrl-d\u003e',  -- <Ctrl-n><Ctrl-d>
    -- 'VSplit',
    -- false                                    -- overwrite=false
  -- )
  -- config.TryBindKey(
    -- '\u003cCtrl-n\u003e\u003cCtrl-s\u003e',  -- <Ctrl-n><Ctrl-s>
    -- 'HSplit',
    -- false                                    -- overwrite=false
  -- )
  -- config.TryBindKey(
    -- '\u003cCtrl-n\u003e\u003cCtrl-a\u003e',  -- <Ctrl-n><Ctrl-a>
    -- 'lua:initlua.VSplitLeft',
    -- false                                    -- overwrite=false
  -- )
  -- config.TryBindKey(
    -- '\u003cCtrl-n\u003e\u003cCtrl-w\u003e',  -- <Ctrl-n><Ctrl-w>
    -- 'lua:initlua.HSplitUp',
    -- false                                    -- overwrite=false
  -- )
-- 
  -- config.TryBindKey('Alt-|', 'lua:initlua.NewView', false)  -- overwrite=false
-- 
  -- linter.makeLinter(
    -- 'flake8-cached',
    -- 'python',
    -- 'flake8-cached',
    -- {'%f'},
    -- '%f:%l:%c: %m'
  -- )
  -- linter.removeLinter('mypy')
  -- linter.makeLinter(
    -- 'mypy',
    -- 'python',
    -- 'mypy',
    -- {
      -- '--disallow-untyped-defs',
      -- '--check-untyped-defs',
      -- '--ignore-missing-imports', '%f'
    -- },
    -- '%f:%l:%c %m'
  -- )
-- 
  -- linter.removeLinter('yaml')
  -- linter.makeLinter('yaml', 'yaml', 'yamllint', {'--format', 'parsable', '%f'}, '%f:%l:%c:%m')
-- end
