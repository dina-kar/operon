import LoamsRouter.Oracle

/-- `loams-router-oracle`: answer JSON-line requests until end of input. -/
partial def loop (stdin : IO.FS.Stream) (stdout : IO.FS.Stream) : IO Unit := do
  let line ← stdin.getLine
  if line.isEmpty then return
  let line := line.trimRight
  if !line.isEmpty then
    let out := match LoamsRouter.Oracle.answer line with
      | .ok j => j.compress
      | .error e => (Lean.Json.mkObj [("ok", false), ("error", "BadRequest"), ("detail", e)]).compress
    stdout.putStrLn out
    stdout.flush
  loop stdin stdout

def main : IO Unit := do
  loop (← IO.getStdin) (← IO.getStdout)
