#!/usr/bin/ruby

#[dspace@ip-10-242-191-240 ~]$ /dspace/bin/dspace metadata-export -f ~/export-11176_1858.csv -i 11176/1858
#[dspace@ip-10-242-191-240 ~]$ /dspace/bin/dspace metadata-import -f ~/export-11176_1858.csv -e helton@un.org -s

# This script exports the metadata information from the DHL Intake Collection in the DSpace repository and then:
# 1) Creates a file suitable for import into ODS
# 

require 'rubygems'
require 'date'
require 'csv'
require 'json'

def log(message)
	logfile = "ods_export.log"
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def export_metadata_to_csv(env)
	# Handle of the collection you're using for intake, e.g., 11176/3045.  Be specific here, just to be sure.
	if env == 'qa'
		intake_collection = "11176/3045"	#this is for the QA environment
	else
		intake_collection = "11176/1858"	#this is for the DEV environment
	end
	output_file = "metadata_export_#{intake_collection.gsub(/\//,'_')}_#{Date.today.to_s}.csv"
	log("Began export operation.  Creating #{output_file} from handle #{intake_collection} in #{env} environment.")
	# Export command, e.g., /dspace/bin/dspace metadata-export
	export_cmd = "/dspace/bin/dspace metadata-export -f #{output_file} -i #{intake_collection}"
	retval = `#{export_cmd}` or log("Unable to execute the export command.")
	log("Export operation completed.")
	return output_file
end

def parse_metadata(csv)
	metadata = Hash.new
	CSV.foreach(csv, { col_sep: ",", encoding: "ISO8859-1", headers: true}) do |row|
	  metadata[row[0]] = row.to_hash
	end
	return metadata
end

def make_ods_file(metadata,map)
	# Do we need the exact time this was generated??  I guess only if we make more than one in a 24 hour period...
	out_file = "ubis-#{Date.today}.csv"
	File.open(out_file, "a+") do |csv|
	  csv.puts "id,issued_date,docsymbol,title,session,agenda,subjects"
	  metadata.each do |m|
	    r = m[1]
	    if r["dc.date.issued"]
	      date_issued = r["dc.date.issued"]
	    elsif r["dc.date.issued[]"]
	      date_issued = r["dc.date.issued[]"]
            else
              date_issued = ""
            end
	    if r["dc.title[en]"]
              title = r["dc.title[en]"]
            elsif r["dc.title"]
              title =  r["dc.title"]
            else
              title = ""
            end
            if r["undr.docsymbol[en]"]
              docsymbol = r["undr.docsymbol[en]"]
            elsif r["undr.docsymbol"]
              docysmbol =  r["undr.docsymbol"]
            else
              docsymbol = ""
            end
	    if r["undr.session[en]"]
		session = r["undr.session[en]"]
	    elsif r["undr.session"]
		 session = r["undr.session"]
	    else
		session = ""
	    end
	    if r["undr.agenda[en]"]
		agenda = r["undr.agenda[en]"]
	    elsif r["undr.agenda"]
		agenda =  r["undr.agenda"]
	    else
		agenda = ""
	    end
	    if r["dc.subject[en]"]
		if map[r["dc.subject[en]"]]
			subject = map[r["dc.subject[en]"]]["id"]
		end
	    elsif r["dc.subject"]
		if map[r["dc.subject"]]
			subject = map[r["dc.subject"]]["id"]
		end
	    else
		subject = ""
	    end
	    csv.puts "\"#{r["id"]}\",\"#{date_issued}\",\"#{docsymbol}\",\"#{title}\",\"#{session}\",\"#{agenda}\",\"#{subject}\""
	  end
	end
end

# Variables we'll need.  These get moved into the functions that make use of them, but it's useful to outline them
# here first.

# Procedural logic
csv = export_metadata_to_csv('dev')
metadata = parse_metadata(csv)
#puts metadata["1866"].inspect
#retarget_items(metadata)

subject_mapfile = "thesaurus-map.json"
map = JSON.parse(File.read("#{subject_mapfile}"))
make_ods_file(metadata,map)
