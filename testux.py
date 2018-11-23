import wx

class SelectAll(wx.Dialog):
    def __init__(self, parent, message, caption, choices):
        wx.Dialog.__init__(self, parent, -1)
        self.SetTitle(caption)
        sizer = wx.BoxSizer(wx.VERTICAL)
        self.message = wx.StaticText(self, -1, message)
        self.clb = wx.CheckListBox(self, -1, choices=choices)
        self.chbox = wx.CheckBox(self, -1, 'Select all')
        self.btns = self.CreateSeparatedButtonSizer(wx.OK | wx.CANCEL)
        self.Bind(wx.EVT_CHECKBOX, self.EvtChBox, self.chbox)

        sizer.Add(self.message, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.clb, 1, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.chbox, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.btns, 0, wx.ALL | wx.EXPAND, 5)
        self.SetSizer(sizer)
        self.Fit()

    def GetSelections(self):
        #return self.clb.GetChecked()
        return  self.clb.GetCheckedItems()

    def EvtChBox(self, event):
        state = self.chbox.IsChecked()
        for i in range(self.clb.GetCount()):
            self.clb.Check(i, state)

    def GetStringSelections(self):
        return self.clb.GetCheckedStrings()


# if __name__ == '__main__':
#     l = ['AND', 'OR', 'XOR', 'NOT']
#     app = wx.App()
#     dlg = SelectAll(None, 'Choose as many as you wish', 'MCD Title', choices = l)
#     if dlg.ShowModal() == wx.ID_OK:
#         result = dlg.GetStringSelections()
#         print type(result)
#         wx.MessageBox(str(result) + ' were chosen')
#     dlg.Destroy()