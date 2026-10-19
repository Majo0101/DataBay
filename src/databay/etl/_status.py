"""Best-effort single-line status output without data-operation side effects."""


class SingleLineStatus:
    def __init__(self, *, carriage_return=False):
        self._disabled = False
        self._handle = None
        self._width = 0
        self._carriage_return = carriage_return
        try:
            from IPython import get_ipython
            from IPython.display import display

            shell = get_ipython()
            self._notebook = shell is not None and getattr(shell, "kernel", None) is not None
            self._display = display
        except Exception:
            # UI failures must not abort credential cleanup or JDBC operations.
            self._disabled = True

    def update(self, message, *, final=False):
        if self._disabled:
            return
        try:
            # Escape terminal controls and nonprinting Unicode, retaining readable names.
            message = "".join(char if char.isprintable() else ascii(char)[1:-1]
                              for char in message)
            if self._notebook:
                if self._handle is None:
                    self._handle = self._display(message, display_id=True)
                else:
                    self._handle.update(message)
            else:
                prefix = "\r" if self._width or self._carriage_return else ""
                print(prefix + message.ljust(self._width),
                      end="\n" if final else "", flush=True)
                self._width = len(message)
        except Exception:
            # Do not log display exceptions: their text is outside our control.
            self._disabled = True
