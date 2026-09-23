fx_version 'cerulean'
games { 'gta5', 'rdr3' }
rdr3_warning 'I acknowledge that this is a prerelease build of RedM, and I am aware my resources *will* become incompatible once RedM ships.'
lua54 'yes'

author 'Feather Framework'
description 'Standalone Lua database API with a minimal mysql2 transport'
name 'feather-mysql'
version '0.2.0'
node_version '22'

-- Deliberately not server_only: a resource that declares dependency 'feather-mysql' has that
-- dependency checked on the client too, and a server_only resource does not exist there.

server_scripts {
    'bridge/index.js',
    'server/config.lua',
    'server/errors.lua',
    'server/validation.lua',
    'server/results.lua',
    'server/logger.lua',
    'server/main.lua',
    'server/compat.lua',
}

-- Optional. Resources on this server import @feather-mysql/lib/MySQL.lua directly. Enable this only
-- for third-party resources you do not want to edit that still import @oxmysql/lib/MySQL.lua or declare
-- dependency 'oxmysql', and only when no real oxmysql is on disk. See README, "Replacing oxmysql".
-- provide 'oxmysql'
