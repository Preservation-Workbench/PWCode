local micro = import("micro")
local config = import("micro/config")
local fmt = import("fmt")

function init()
        config.MakeCommand("grow", paneGrow, config.NoComplete)
        config.MakeCommand("shrink", paneShrink, config.NoComplete)

        config.TryBindKey("Alt-R", "command:shrink", false)
        config.TryBindKey("Alt-T", "command:grow", false)
end

function resize(bp, n)
        local tab = bp:Tab()
        if #tab.Panes < 1 then
                return
        end
        local id = tab.Panes[2]:ID()
        local node = tab:GetNode(id)
        tab.Panes[1]:ResizePane(node.X + n)
end

function paneGrow(bp)
        resize(bp, 3)
end

function paneShrink(bp)
        resize(bp, -3)
end