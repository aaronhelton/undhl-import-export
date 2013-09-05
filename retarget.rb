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
	
end

def add_to_undo(row)

end

def parse_csv(csv)
	metadata = Hash.new
	CSV.foreach(csv, { col_sep: ",", encoding: "ISO8859-1", headers: true}) do |row|
		metadata[row[0]] = row.to_hash
	end
	return metadata
end

def remap(metadata, patternfile)
	metadata.each do |m|
		r = m[1]
		item_id = r["id"]
		collection = r["collection"]
		docsymbol = r["undr.docsymbol[en]"]
		File.read(patternfile) do |p|
			pattern = Regexp.escape(p)
			if /^#{pattern}/.match(docsymbol)
				p "#{docsymbol} contains #{pattern}"
			end
		end
	end
	return metamap
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
	opt :undo_file, "File containing reversal data to undo a previous move.", :type => String
	opt :verbose, "In case you want to review and confirm changes."
	opt :dev, "Flag to set the dev system.  Normally this is run on the QA system, so you don't need to specify it."
end
Trollop::die :eperson, "is required." unless opts[:eperson]
Trollop::die :pattern_file, "is not readable or does not exist." unless File.exists?(opts[:pattern_file]) if opts[:pattern_file]
Trollop::die :undo_file, "is not readable or does not exist." unless File.exists?(opts[:undo_file]) if opts[:undo_file]

if opts[:dev]
	intake_community = ""
else
	intake_community = "11176/3045"
end
outfile = "metadata_export_#{intake_community.gsub(/\//,'_')}-#{Date.today.to_s}.csv"
metadata_export = "/dspace/bin/dspace metadata-export -f #{outfile} -i #{intake_community}"
metadata_import = ''

# Steps
# 1.  Run metadata export
# 2.  Extract Item IDs and Document Symbols.
# 3.  Construct symbol-based mapping
# 4.  Generate new CSV for metadata import
# 5.  Import metadata.

if !File.exists?(outfile)
	# We assume that if the export to ODS was already run, we use that.  Otherwise we get another one.  This assumption may not be valid.
	`#{metadata_export}` or abort "The program encountered an issue executing the following command: #{metadata_export}.  Please check your permissions and try again."
else
	metadata = parse_csv(outfile)
end

if metadata
	remap(metadata, opts[:pattern_file]) or abort "Remapping could not be completed..."
end