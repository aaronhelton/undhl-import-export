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
require 'trollop'
require 'mail'

def log(message)
	logfile = "logs/itp_daily.log"
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def export_metadata_to_csv(intake_collection)
	# Handle of the collection you're using for intake, e.g., 11176/3045.  Be specific here, just to be sure.
	#if env == 'qa'
	#	intake_collection = "11176/3045"	#this is for the QA environment
	#else
	#	intake_collection = "11176/1858"	#this is for the DEV environment
	#end
	output_file = "metadata_export_#{intake_collection.gsub(/\//,'_')}_#{Date.today.to_s}.csv"
	log("Began export operation.  Creating #{output_file} from handle #{intake_collection}.")
	# Export command, e.g., /dspace/bin/dspace metadata-export
	export_cmd = "/dspace/bin/dspace metadata-export -f #{output_file} -i #{intake_collection}"
	retval = `#{export_cmd}` or log("Unable to execute the export command.")
	log("Export operation completed.")
	return output_file
end

def parse_metadata(csv)
	log("Parsing metadata located in #{csv}.")
	metadata = Hash.new
	CSV.foreach(csv, { col_sep: ",", encoding: "ISO8859-1", headers: true}) do |row|
	  metadata[row[0]] = row.to_hash
	end
	return metadata
end

def generate_report(metadata)
        report = "The following items have been processed and are now available in the DHL Intake collection.\n\n"
	log("Generating report")
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
	  if r["dc.identifier.uri"]
            uri = r["dc.identifier.uri"]
          elsif r["dc.identifier.uri[]"]
            uri = r["dc.identifier.uri[]"]
          end
	  report = report + <<-EOS
Date Issued: #{date_issued}
Document Symbol(s): #{docsymbol}
Title: #{title}
URI: #{uri}
\n\n
          EOS
	end
	log("Report: #{report}")
	return report
end

def email_report(report, creds, recipients)
  # This should never, EVER trigger, but just in case...
  Trollop::die "There are no valid recipients." unless recipients

  recipients.each do |r|
    mail = Mail.new do
      from 	'un.dhl.dag@gmail.com'
      to	r
      subject	"Daily list of items accepted into DAG Digital Assets Gateway"
      body	report
    end
    mail.delivery_method :smtp, {	:address	=>	'smtp.gmail.com',
					:port		=> 	25,
					:user_name	=>	creds[:user_name],
					:password	=> 	creds[:password] }
    mail.deliver!
  end
end


opts = Trollop::options do
  banner <<-EOS
itp_daily makes a daily list of items that have been processed and made available in the designated DSpace intake collection.  It provides nothing more than a list of what is there currently, so unless items are moved out, the list will continue to grow.

The resulting report can be printed to screen (default) or emailed to a list of recipients.

Usage:
	itp_daily.rb [options]

where options are:
EOS

  opt :intake_handle, "Specify the handle of the intake collection.  Required.", :type => String
  opt :email, "Tells itp_daily to send the results via email."
  opt :recipients, "Specify one or more email addresses as report recipients.  At least one address is required when using the --email flag.", :type => :strings
  opt :credentials, "Specify a file containing a username and password combination, separated by '::' and having file permissions 0600", :type => String
  opt :debug, "For extra visibility into the script and its functions."

end
Trollop::die :intake_handle, "is a required argument.  Please specify it" unless opts[:intake_handle]
Trollop::die :recipients, "are required when the --email flag is set" unless opts[:recipients] if opts[:email]
Trollop::die :credentials, "file is required when using email delivery" unless opts[:credentials] if opts[:email]
Trollop::die :credentials, "file not found.  Check the path and try again" unless File.exists?(opts[:credentials]) if opts[:credentials]
Trollop::die :credentials, "file is world readable.  chmod 600 #{opts[:credentials]} and invoke this script again" if opts[:credentials] && File.world_writable?(opts[:credentials])

#Precaution included with all scripts in this package.  No telling what order things get executed in.
if !File.exists?('logs')
        Dir.mkdir('logs') or abort "Unable to create logs directory.  Make sure you have permission to write to it and try again."
end

if opts[:debug]
  p opts
end

# Since our DSpace handles have a format like ddddd/dddd or 12345/6789, the match pattern should be \A\d+\/\d+
csv = ''
if opts[:intake_handle] =~ /\A\d+\/\d+/
  csv = export_metadata_to_csv(opts[:intake_handle])
else
  Trollop::die :intake_handle, "for this repository is expected to take the format ddddd/dddd.  For example: 11176/2031"
end


metadata = parse_metadata(csv) if csv
report = generate_report(metadata) if metadata

creds = Hash.new

if opts[:credentials]
  creds[:user_name] = File.read(opts[:credentials]).split(/::/)[0].strip
  creds[:password] = File.read(opts[:credentials]).split(/::/)[1].strip
  p creds if opts[:debug]
end

if opts[:email] && opts[:recipients]
  # Generate and email the report.  Suppress local/screen output.
  # Note: There is no validation of email addresses here.  There is no regex capable of validating all valid email addresses.  
  # Let the mail server sort it out.
  email_report(report,creds,opts[:recipients]) 
else
  # Generate the report and print to screen.
  p report
end
