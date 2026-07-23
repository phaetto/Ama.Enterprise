namespace Ama.Enterprise.Monitoring.Cli.UI;

using System;
using System.Collections.Generic;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

internal sealed class CommandPane : FrameView
{
    private readonly IList<string> commandResults;
    private readonly TextView outputTextView;
    private readonly TextField commandInput;

    public CommandPane()
    {
        Title = "Commands";
        X = 0;
        Y = 0;
        Width = Dim.Percent(50);
        Height = Dim.Fill();

        commandResults = new List<string> { "System ready. Type a command and press Enter." };

        outputTextView = new TextView
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill() - 3, // Leave 3 lines for the input frame at the bottom
            ReadOnly = true,
            Text = string.Join(Environment.NewLine, commandResults)
        };

        var inputFrame = new FrameView
        {
            Title = "Execute",
            X = 0,
            Y = Pos.AnchorEnd(3),
            Width = Dim.Fill(),
            Height = 3
        };

        commandInput = new TextField
        {
            Text = "",
            X = 0,
            Y = 0,
            Width = Dim.Fill()
        };

        commandInput.KeyDown += (sender, args) =>
        {
            var keyStr = args?.ToString() ?? string.Empty;
            if (keyStr.Contains("Enter") || keyStr.Contains("Return"))
            {
                var input = commandInput.Text?.ToString();
                
                if (!string.IsNullOrWhiteSpace(input))
                {
                    commandResults.Add($"> {input}");
                    commandResults.Add("Executing..."); 

                    outputTextView.Text = string.Join(Environment.NewLine, commandResults);
                    commandInput.Text = "";
                }
            }
        };

        inputFrame.Add(commandInput);
        Add(outputTextView, inputFrame);
    }
}