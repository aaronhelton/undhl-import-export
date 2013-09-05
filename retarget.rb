#!/usr/bin/ruby
# encoding: UTF-8

#Assumes ruby v 2.0 or greater.  On *nix systems, you might want to use Ruby Version Manager (rvm) to install different concurrent ruby versions
# For example:
# \curl -L https://get.rvm.io | bash
# source .rvm/scripts/rvm
# rvm list known
# rvm use 2.0.0-p195 --default

require 'rubygems'
require 'csv'	#External ruby gem.  gem install csv to install it
require 'net/http'
require 'date'
require 'trollop'	#External ruby gem.  gem install trollop to install it

def add_to_map(row)
	filename = "map-#{Date.today.to_s}.csv"
	File.open(filename, "a+") do |f|
		f.puts row
	end
end

def add_to_undo(row)
	filename = "undo-#{Date.today.to_s}.csv"
	File.open(filename, "a+") do |f|
		f.puts row
	end
end

def parse_csv(csv)
	metadata = Hash.new
	CSV.foreach(csv, { col_sep: ",", encoding: "ISO8859-1", headers: true}) do |row|
		metadata[row[0]] = row.to_hash
	end
	return metadata
end

def remap(metadata, patternfile)
	patterns = Array.new
	add_to_map("id,collection")
	add_to_undo("id,collection")
	text = File.open(patternfile).read
	text.each_line do |line|
		patterns << line.split('::')
	end
	metadata.each do |m|
		r = m[1]
		item_id = r["id"]
		collection = r["collection"]
		docsymbol = r["undr.docsymbol[en]"]
		patterns.each do |p|
			pattern = Regexp.escape(p[0])
			if /^#{pattern}/.match(docsymbol)
				row = "#{item_id},#{p[1]}"
				add_to_map(row)
				undo = "#{item_id},#{collection}"
				add_to_undo(undo)
			end
		end
	end
	#return metamap
end

##### Begin ARGV Processing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
retarget.rb moves completed DSpace submissions from the intake collection to one or more target collections based on patterns within the Document Symbol.

Usage:
	retarget.rb [options]

where options are:
EOS

	opt :pattern_file, "File containing Document Symbol patterns and target collection handles.", :type => String, :default => 'lib/dspatterns.txt'
	opt :eperson, "DSpace eperson email address used to perform the item moves.", :type => String
	opt :verbose, "In case you want to review and confirm changes."
	opt :dev, "Flag to set the dev system.  Normally this is run on the QA system, so you don't need to specify it."
end
Trollop::die :eperson, "is required." unless opts[:eperson]
Trollop::die :pattern_file, "is not readable or does not exist." unless File.exists?(opts[:pattern_file]) if opts[:pattern_file]

if opts[:dev]
	intake_collection = ""
else
	intake_collection = "11176/3045"
end
outfile = "metadata_export_#{intake_collection.gsub(/\//,'_')}-#{Date.today.to_s}.csv"

# Steps
# 1.  Run metadata export
# 2.  Extract Item IDs and Document Symbols.
# 3.  Construct symbol-based mapping
# 4.  Generate new CSV for metadata import
# 5.  Import metadata.

if !File.exists?(outfile)
	# We assume that if the export to ODS was already run, we use that.  Otherwise we get another one.  This assumption may not be valid.
	`#{metadata_export}` or abort "The program encountered an issue executing the following command: #{metadata_export}.  Please check your permissions and try again."
end
metadata = parse_csv(outfile)


if metadata
	remap(metadata, opts[:pattern_file]) or abort "Remapping could not be completed..."
end

if File.exists?("map-#{Date.today.to_s}.csv") && File.exists?("undo-#{Date.today.to_s}.csv")
	if opts[:verbose]
		metadata_import = "/dspace/bin/dspace metadata-import -f map-#{Date.today.to_s}.csv -e #{opts[:eperson]}"
	else
		metadata_import = "/dspace/bin/dspace metadata-import -f map-#{Date.today.to_s}.csv -e #{opts[:eperson]} -s"
	end
	`#{metadata_import}` or abort "Unable to complete import."
end
