#!/usr/bin/ruby
# encoding: UTF-8

require 'rubygems'
require 'csv'	#External ruby gem.  gem install csv to install it
require 'net/http'
require 'date'
require 'trollop'	#External ruby gem.  gem install trollop to install it

##### Begin ARGV Processing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
retarget.rb moves completed DSpace submissions from the intake collection to one or more target collections based on patterns within the Document Symbol.

Usage:
	retarget.rb [options]

where options are:
EOS
	:pattern_file, "File containing Document Symbol patterns and target collection handles.", :type => String, :default => 'lib/dspatterns.txt'
	:eperson, "DSpace eperson email address used to perform the item moves.", :type => String
	:undo_file, "File containing reversal data to undo a previous move.", :type => String
	:verbose, "In case you want to review and confirm changes."
end
Trollop::die :eperson, "is required." unless opts[:eperson]
Trollop::die :pattern_file, "is not readable or does not exist." unless File.exists?(opts[:pattern_file]) if opts[:pattern_file]
Trollop::die :undo_file, "is not readable or does not exist." unless File.exists?(opts[:undo_file]) if opts[:undo_file]

