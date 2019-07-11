God.watch do |w|
  w.name = "one"
  w.start = "one-heroku.py"
  w.keepalive
end

God.watch do |w|
  w.name = "twor"
  w.start = "two-heroku.py"
  w.keepalive
end
