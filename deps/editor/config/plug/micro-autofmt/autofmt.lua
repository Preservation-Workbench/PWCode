VERSION = "2.0.0"

local config = import("micro/config")
local shell = import("micro/shell")
local filepath = import("path/filepath")
local micro = import("micro")

local fmtCommands = {}
fmtCommands["python"] = "yapf -i"
fmtCommands["c"]      = "clang-format -i"
fmtCommands["c++"]    = "clang-format -i"
fmtCommands["csharp"] = "clang-format -i"
fmtCommands["racket"] = "raco fmt --width 80 --max-blank-lines 2 -i"
fmtCommands["javascript"] = "prettier --write --loglevel silent"
fmtCommands["rust"] = "rustfmt +nightly"
fmtCommands["shell"] = "shfmt -w"

function init()
    config.RegisterCommonOption("autofmt", "fmt-onsave", true)
    config.MakeCommand("fmt", doFmt, config.NoComplete)
    config.AddRuntimeFile("autofmt", config.RTHelp, "help/autofmt.md")
end

function onSave(bp)
    -- if not set by user, it is nil
    if bp.Buf.Settings["autofmt.fmt-onsave"] == nil then
        if config.GetGlobalOption("autofmt.fmt-onsave") then
            doFmt(bp)
        end
    else
        if bp.Buf.Settings["autofmt.fmt-onsave"] then
            doFmt(bp)
        end
    end
    -- micro.InfoBar():Message("autofmt.fmt-onsave ","global:",config.GetGlobalOption("autofmt.fmt-onsave")," local:",bp.Buf.Settings["autofmt.fmt-onsave"])
end

function doFmt(bp)
    if fmtCommands[bp.Buf:FileType()] ~= nil then
        local fmtTool = fmtCommands[bp.Buf:FileType()]:gsub("%s+.+", "")
        local _, err = shell.ExecCommand("sh", "-c", "command -v "..fmtTool)
        if err == nil then
            local dirPath, _ = filepath.Split(bp.Buf.AbsPath)
            bp:Save()
            local output, err = shell.ExecCommand("sh","-c","cd \"" .. dirPath .. "\"&& " .. fmtCommands[bp.Buf:FileType()] .. " " .. bp.Buf.AbsPath)
            if err == nil then
                bp.Buf:ReOpen()
            else
                if bp.Buf:FileType() ~= 'shell' then
                    micro.InfoBar():Error("autofmt: "..output)
                end
            end
        else
            micro.InfoBar():Error("autofmt: "..bp.Buf:FileType().." formatter "..fmtTool.." not found in $PATH")
        end
    end
end