dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local exitgrab = false
local exit_url = false

local discovered = {}
local found_second = false

local bad_items = {}

if not urlparse or not http then
  io.stdout:write("socket not correctly installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(abort)
  abort = true
  if abort then
    abortgrab = true
  end
  exitgrab = true
  if not bad_items[item_name] then
    io.stdout:write("Aborting item " .. item_name .. ".\n")
    io.stdout:flush()
    bad_items[item_name] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

submit_discovered = function()
  local newitems = nil
  for item, _ in pairs(discovered) do
    io.stdout:write("Queuing item " .. item .. ".\n")
    io.stdout:flush()
    if newitems == nil then
      newitems = item
    else
      newitems = newitems .. "\0" .. item
    end
  end
  if newitems ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/liveleak-ky9xnba3bqx5s8w/",
        newitems
      )
      if code == 200 or code == 409 then
        break
      end
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abort_item()
    end
  end
end

get_item = function(url)
  local match = string.match(url, "^https?://[^/]*liveleak%.com/view%?[ti]=([0-9a-zA-Z_%-]+_[0-9]+)")
  local type_ = "video-old"
  if not match then
    match = string.match(url, "^https?://[^/]*liveleak%.com/v%?t=([0-9a-zA-Z_%-]+)")
    type_ = "video-new"
  end
  if not match then
    match = string.match(url, "^https?://[^/]*liveleak%.com/e/([0-9a-zA-Z_%-]+)")
    type_ = "embed-old"
    if not match then
      match = string.match(url, "^https?://[^/]*liveleak%.com/ll_embed%?f=([0-9a-zA-Z_%-]+)")
    end
  end
  if not match then
    match = string.match(url, "^https?://[^/]*liveleak%.com/e%?t=([0-9a-zA-Z_%-]+)")
    type_ = "embed-new"
  end
  if not match then
    match = string.match(url, "^https?://[^/]*liveleak.com/list%?q=([^&]+)")
    type_ = "user"
  end
  if match then
    return type_, match
  end
end

allowed = function(url, parenturl)
  if string.match(url, "language_code=")
    or string.match(url, "theme_id=") then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  if string.match(url, "^https?://cdn[0-9]*%.liveleak%.com/") then
    return true
  end

  local type_, match = get_item(url)
  if ((type_ == "embed-new" and item_type == "video-new")
    or (type_ == "video-new" and item_type == "embed-new")
    or (type_ == "embed-old" and item_type == "video-old")
    or (type_ == "video-old" and item_type == "embed-old"))
    and not ids[match]
    and not found_second then
    print(type_, item_type, match)
    ids[match] = true
    found_second = true
  elseif match and not ids[match] then
    discovered[type_ .. ":" .. match] = true
  end

  for _, p in pairs({"([0-9a-zA-Z_%-]+)", "([0-9]+)", "([^%?&]+)"}) do
    for s in string.gmatch(url, p) do
      if ids[s] then
        return true
      end
    end
  end

  match = string.match(url, "/c/([^%?&]+)")
  if match then
    discovered["user:" .. match] = true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  if is_css then
    return urls
  end
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.gsub(url_, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])", function (s)
      local i = tonumber(s, 16)
      if i < 128 then
        return string.char(i)
      else
        -- should not have these
        abort_item()
      end
    end)
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    --url_ = string.match(url_, "^(.-)/?$")
    url_ = string.match(url_, "^(.-)\\?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^/>")
      or string.match(newurl, "^/&gt;")
      or string.match(newurl, "^/<")
      or string.match(newurl, "^/&lt;")
      or string.match(newurl, "^/%*") then
      return false
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    checknewurl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local type_, match = get_item(url)
  if type_ == "video-old" then
    check(string.gsub(url, "/view%?[ti]=", "/view%?t="))
    check(string.gsub(url, "/view%?[ti]=", "/view%?i="))
  elseif type_ == "embed-old" then
    check(string.gsub(url, "/e/", "/ll_embed%?f="))
    check(string.gsub(url, "/ll_embed%?f=", "/e/"))
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://cdn[0-9]*%.liveleak%.com/") then
    html = read_file(file)
    for user in string.gmatch(html, "Credit: ([^<%s]+)%s*<") do
      discovered["user:" .. user] = true
    end
    for location in string.gmatch(html, "Location: ([^<%s]+)%s*<") do
      discovered["location:" .. location] = true
    end
    local tags = string.match(html, "<p><strong>Tags:</strong>([^<]+)</p>")
    if tags then
      for tag in string.gmatch(tags, "([^,]+)") do
        discovered["tag:" .. string.gsub(string.match(tag, "^%s*(.-)%s*$"), " ", "+")] = true
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ':%s*url%(([^%)"]+)%)') do
      checknewurl(newurl)
    end
  end

  return urls
end

set_new_item = function(url)
  type_, match = get_item(url)
  if match and not ids[match] then
    abortgrab = false
    exitgrab = false
    ids[match] = true
    item_value = match
    item_type = type_
    item_name = type_ .. ":" .. match
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if abortgrab or exitgrab then
    abort_item(true)
    submit_discovered()
    return wget.actions.ABORT
    --return wget.actions.EXIT
  end

  set_new_item(url["url"])
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if exitgrab then
    return wget.actions.EXIT
  end

  if status_code == 404 and string.match(url["url"], "%.mp4") then
    abort_item(true)
    submit_discovered()
    return wget.actions.ABORT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc] then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if status_code == 0
    or (status_code >= 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 1
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      abort_item(true)
      submit_discovered()
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for item, _ in pairs(bad_items) do
    file:write(item .. "\n")
  end
  file:close()
  submit_discovered()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  submit_discovered()
  if abortgrab then
    abort_item(true)
    return wget.exits.IO_FAIL
  end
  return exit_status
end

