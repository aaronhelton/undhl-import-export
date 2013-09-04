#!/usr/bin/ruby
# encoding: UTF-8

# This script should be run with ruby version > 1.9.3 and the following libraries and gems

require 'rubygems'
require 'csv'	#External ruby gem.  gem install csv to install it
require 'net/http'
require 'aws/s3'	#External ruby gem.  gem install aws-s3 to install it
require 'date'
require 'pdf-reader'	#External ruby gem.  gem install pdf-reader to install it
require 'mimemagic'	#External ruby gem.  gem install mimemagic to install it.
require 'trollop'	#External ruby gem.  gem install trollop to install it


def log(message)
	logfile = "import_from_dgacm.log"
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def err(message)
	# Need a separate error log to catch things like incomplete packages so we don't import with missing or corrupt files
	logfile = "import_errors.log"
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def fails(data)
	# This one outputs a CSV of metadata that can be re-processed later (specified via command line)
	
end

def lxcode(language)
	case language
		when "Arabic"
			iso = "AR"
		when "Chinese"
			iso = "ZH"
		when "English"
			iso = "EN"
		when "French"
			iso = "FR"
		when "Russian"
			iso = "RU"
		when "Spanish"
			iso = "ES"
	end
	return iso
end

def getext(mimetype)
	debug = 0
	if debug == 1
		puts "Got mimetype #{mimetype}"
	end
	if mimetype == 'application/msword'
		ext = "doc"
	elsif mimetype.to_s == 'application/pdf'
			ext = "pdf"
	else
		log("UNABLE to determine proper file extension for MIME Type: #{mimetype}.  The file is likely corrupt or incomplete.")
		ext = "err"
	end	
	if debug == 1
		puts "Assigning extension #{ext}"
	end
	return ext
end

#Function definitions.  Names should be self-explanatory
def get_latest_csv(access_key_id, secret_access_key, csv_type).
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)

	files = AWS::S3::Bucket.find('undhl-dgacm').objects( :prefix => 'Drop' )

	epoch_time = DateTime.parse('1970-01-01T00:00:00+00:00')
	latest_time = epoch_time
	latest_file = ''

	files.each do |file|
		if file.key.downcase =~ /^drop\/#{csv_type}/
			mtime = DateTime.parse("#{file.about["last-modified"]}")
			if mtime > latest_time
				latest_file = file.key
			end
		end
	end
	log("Getting latest #{csv_type} file, found #{latest_file.gsub(/Drop\//,'')}")
	File.open(latest_file.gsub(/Drop\//,''), 'w') do |f|
		AWS::S3::S3Object.stream(latest_file, 'undhl-dgacm') do |chunk|
			f.write chunk
		end
	end
	return latest_file.gsub(/Drop\//,'')
end

def get_specific_csv(access_key_id, secret_access_key, filename)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)

	#files = AWS::S3::Bucket.find('undhl-dgacm').objects( :prefix => 'Drop' )
	if AWS::S3::S3Object.exists?("#{filename}",'undhl-dgacm')
		log("Found #{filename} in S3 bucket.")
		File.open(latest_file.gsub(/Drop\//,''), 'w') do |f|
			AWS::S3::S3Object.stream(filename, 'undhl-dgacm') do |chunk|
				f.write chunk
			end
		end
	else
		Trollop::die "Remote file doesn't exist.  Check the supplied filename and try again."
	end
end

def parse_csv(csv_file)
	# These CSVs have multiple rows per document symbol.  We need to combine them or otherwise figure out a way to deal with them.
	# Some of the documents have multiple entries per job number, even.  I think we should just take the final entry for each job number...
	# Also, some of the document symbol fields contain multiple document symbols.  This isn't a big issue, but we have to account for it.
	
	ods_daccess = '157.150.172.70'			
	# may need to set this in c:\windows\system32\drivers\etc\hosts:
	# 157.150.172.70  daccess-ods.un.org
	# If so, then uncommend the following line and comment out the previous definition.
	#ods_daccess = 'daccess-ods.un.org'
	
	debug = 0		#Set this to 0 unless you *really* need it.
	
	transitional = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	CSV.foreach(csv_file, { col_sep: ",", encoding: "ISO8859-1", headers: true}) do |row|
		if debug == 1 then log("Got row #{row}") end
		#Key off of docsymbol; only the first if there are multiples.
		if row[4].strip =~ /\s\s/  
			docsymbol = row[4].strip.split(/\s+/)[0]
		else
			docsymbol = row[4].strip
		end
		l = row[3].strip
		case l
			when "A"
				language = "Arabic"
			when "C"
				language = "Chinese"
			when "E"
				language = "English"
			when "F"
				language = "French"
			when "R"
				language = "Russian"
			when "S"
				language = "Spanish"
		end
		transitional[docsymbol][language] = row.to_hash
	end
	metadata = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	docsymbols = transitional.keys
	docsymbols.each do |ds|
		languages = transitional[ds].keys
		# Get the non-unique stuff first
		if transitional[ds].key?("English") 
			if transitional[ds]["English"]["DISTRIBUTION"].strip == "GENERAL"
				if 	transitional[ds]["English"]["AGEN_ITEM"]
					metadata[ds]["agen_item"] = transitional[ds]["English"]["AGEN_ITEM"].strip
				end
				if 	transitional[ds]["English"]["AGEN_SUB_ITEM"]
					metadata[ds]["agen_sub_item"] = transitional[ds]["English"]["AGEN_SUB_ITEM"].strip
				end
				metadata[ds] = {
				"doc_num" => transitional[ds]["English"]["DOC_NUM"].strip,
				"title" => transitional[ds]["English"]["TITLE"].strip,
				"symbols" => transitional[ds]["English"]["SYMBOLS"].strip.split(/\s+/),
				"distribution" => transitional[ds]["English"]["DISTRIBUTION"].strip,
				"isbn" => transitional[ds]["English"]["ISBN"].to_s.strip,
				"issn" => transitional[ds]["English"]["ISSN"].to_s.strip,
				"cr_sales_num" => transitional[ds]["English"]["CR_SALES_NUM"].to_s.strip,
				"issued_date" => Date.strptime(transitional[ds]["English"]["ISSUED_DATE"].strip.split(/\s/)[0], '%m/%d/%y').to_s,
				"slot_num" => transitional[ds]["English"]["SLOT_NUM"].strip,
				"languages" =>  languages }
			
				#Get the non-unique stuff next
				languages.each do |language|
					jn = transitional[ds][language]["JOB_NUM"].strip.gsub(/NY-J-/,'N').gsub(/-/,'').gsub(/LP/,'').gsub(/\*/,'').gsub(/\/1/,'').gsub(/X/,'')
					jp = jn[0..2] + "/" + jn[3..5] + "/" + jn[6..7]
					url = ods_daccess + "/doc/UNDOC/GEN/" + jp + "/PDF/" + jn + ".pdf"
					metadata[ds][language] = { 
						"job_num" => transitional[ds][language]["JOB_NUM"].strip,
						"language" => language,
						"url" => url
					}
				end
			end
		end
	end
	return metadata
end		

def package(metadata)
	package_dir = 'package-' + Date.today.to_s
	if !File.exists?(package_dir)
		Dir.mkdir(package_dir) or abort "Unable to create #{package_dir}.  Make sure you have permission to write to it and try again."
	end
	docsymbols = metadata.keys
	log("Processing #{docsymbols.length} document symbols.")
	docsymbols.each do |ds|
		contents = Array.new
		dsfn = ds.gsub(/\//,'-')
		out_dir = package_dir + "/" + ds.gsub(/\//,'_')
		if !File.exists?(out_dir)
			Dir.mkdir(out_dir) or abort "Unable to create #{out_dir}.  Make sure you have permission to write to it and try again."
		end
		#Make a dublin_core file
		entity_replace = '<!ENTITY quot   "&#34;"><!ENTITY nbsp   "&#160;"><!ENTITY iexcl  "&#161;"><!ENTITY cent   "&#162;"><!ENTITY pound  "&#163;"><!ENTITY curren "&#164;"><!ENTITY yen   "&#165;"><!ENTITY brvbar "&#166;"><!ENTITY sect   "&#167;"><!ENTITY uml    "&#168;"><!ENTITY copy   "&#169;"><!ENTITY ordf   "&#170;"><!ENTITY laquo  "&#171;"><!ENTITY not    "&#172;"><!ENTITY shy    "&#173;"><!ENTITY reg    "&#174;"><!ENTITY macr   "&#175;"><!ENTITY deg    "&#176;"><!ENTITY plusmn "&#177;"><!ENTITY sup2   "&#178;"><!ENTITY sup3   "&#179;"><!ENTITY acute  "&#180;"><!ENTITY micro  "&#181;"><!ENTITY para   "&#182;"><!ENTITY middot "&#183;"><!ENTITY cedil  "&#184;"><!ENTITY sup1   "&#185;"><!ENTITY ordm   "&#186;"><!ENTITY raquo  "&#187;"><!ENTITY frac14 "&#188;"><!ENTITY frac12 "&#189;"><!ENTITY frac34 "&#190;"><!ENTITY iquest "&#191;"><!ENTITY Agrave "&#192;"><!ENTITY Aacute "&#193;"><!ENTITY Acirc  "&#194;"><!ENTITY Atilde "&#195;"><!ENTITY Auml   "&#196;"><!ENTITY Aring  "&#197;"><!ENTITY AElig  "&#198;"><!ENTITY Ccedil "&#199;"><!ENTITY Egrave "&#200;"><!ENTITY Eacute "&#201;"><!ENTITY Ecirc  "&#202;"><!ENTITY Euml   "&#203;"><!ENTITY Igrave "&#204;"><!ENTITY Iacute "&#205;"><!ENTITY Icirc  "&#206;"><!ENTITY Iuml  "&#207;"><!ENTITY ETH    "&#208;"><!ENTITY Ntilde "&#209;"><!ENTITY Ograve "&#210;"><!ENTITY Oacute "&#211;"><!ENTITY Ocirc  "&#212;"><!ENTITY Otilde "&#213;"><!ENTITY Ouml  "&#214;"><!ENTITY times  "&#215;"><!ENTITY Oslash "&#216;"><!ENTITY Ugrave "&#217;"><!ENTITY Uacute "&#218;"><!ENTITY Ucirc  "&#219;"><!ENTITY Uuml   "&#220;"><!ENTITY Yacute "&#221;"><!ENTITY THORN  "&#222;"><!ENTITY szlig  "&#223;"><!ENTITY agrave "&#224;"><!ENTITY aacute "&#225;"><!ENTITY acirc  "&#226;"><!ENTITY atilde "&#227;"><!ENTITY auml   "&#228;"><!ENTITY aring  "&#229;"><!ENTITY aelig  "&#230;"><!ENTITY ccedil "&#231;"><!ENTITY egrave "&#232;"><!ENTITY eacute "&#233;"><!ENTITY ecirc  "&#234;"><!ENTITY euml   "&#235;"><!ENTITY igrave "&#236;"><!ENTITY iacute "&#237;"><!ENTITY icirc  "&#238;"><!ENTITY iuml   "&#239;"><!ENTITY eth    "&#240;"><!ENTITY ntilde "&#241;"><!ENTITY ograve "&#242;"><!ENTITY oacute "&#243;"><!ENTITY ocirc  "&#244;"><!ENTITY otilde "&#245;"><!ENTITY ouml   "&#246;"><!ENTITY divide "&#247;"><!ENTITY oslash "&#248;"><!ENTITY ugrave "&#249;"><!ENTITY uacute "&#250;"><!ENTITY ucirc  "&#251;"><!ENTITY uuml   "&#252;"><!ENTITY yacute "&#253;"><!ENTITY thorn  "&#254;"><!ENTITY yuml   "&#255;">'
		File.open("#{out_dir}/dublin_core.xml", "w+") do |dubc|
			dubc.puts '<?xml version="1.0" encoding="UTF-8"?>'
			dubc.puts '<!DOCTYPE content ['
			dubc.puts entity_replace
			dubc.puts ']>'
			dubc.puts '<dublin_core>'
			if metadata[ds]["title"] && metadata[ds]["title"] != 'NULL'
				dubc.puts '  <dcvalue element="title" qualifier="none">' + metadata[ds]["title"] + '</dcvalue>'
			else
				dubc.puts '  <dcvalue element="title" qualifier="none">' + ds + '</dcvalue>'
			end
			# issued date needs to be in yyyy-mm-dd format; it should not have arrived null, but it might be a good idea to check it.
			dubc.puts '  <dcvalue element="date" qualifier="issued">' + metadata[ds]["issued_date"] + '</dcvalue>'
			if metadata[ds]["isbn"] && metadata[ds]["isbn"] != 'NULL'
				dubc.puts '  <dcvalue element="identifier" qualifier="isbn">' + metadata[ds]["isbn"] + '</dcvalue>'
			end
			if metadata[ds]["issn"] && metadata[ds]["isbn"] != 'NULL'
				dubc.puts '  <dcvalue element="identifier" qualifier="issn">' + metadata[ds]["issn"] + '</dcvalue>'
			end
			metadata[ds]["languages"].each do |dc_language|
				dubc.puts '  <dcvalue element="language" qualifier="none">' + dc_language + '</dcvalue>'
			end
			dubc.puts '  <dcvalue element="type" qualifier="none">UN resolutions/decisions, UN draft resolutions/decisions</dcvalue>'
			dubc.puts '</dublin_core>'
		end
		#Make a metadata_undr file
		File.open("#{out_dir}/metadata_undr.xml", "w+") do |undr|
			undr.puts '<?xml version="1.0" encoding="UTF-8"?>'
			undr.puts '<!DOCTYPE content ['
			undr.puts entity_replace
			undr.puts ']>'
			undr.puts '<dublin_core schema="undr">'
			undr.puts '  <dcvalue element="docsymbol" qualifier="none">' + ds + '</dcvalue>'
			if metadata[ds]["agen_item"] && metadata[ds]["agen_item"] != 'NULL'
				if metadata[ds]["agen_sub_item"] && metadata[ds]["agen_sub_item"] != 'NULL'
					undr.puts '  <dcvalue element="agenda" qualifier="none">' + metadata[ds]["agen_item"] + ": " + metadata[ds]["agen_sub_item"] + '</dcvalue>'
				else
					undr.puts '  <dcvalue element="agenda" qualifier="none">' + metadata[ds]["agen_item"] + '</dcvalue>'
				end
			end
			undr.puts '</dublin_core>'
		end
		#Fetch the documents and describe them in contents
		metadata[ds]["languages"].each do |language|
			#Determine bitstream order
			case language
				when "English"
					bitstream_order = 1
				when "French"
					bitstream_order = 2
				when "Russian"
					bitstream_order = 3
				when "Spanish"
					bitstream_order = 4
				when "Arabic"
					bitstream_order = 5
				when "Chinese"
					bitstream_order = 6
				else
					bitstream_order = 7
			end
			parts = metadata[ds][language]["url"].gsub(/http\:\/\//, "").split("/")
			host = parts[0]
			path = "/" + [parts[1],parts[2],parts[3],parts[4],parts[5],parts[6],parts[7],parts[8]].join("/")
			filename = parts[8]
			log("Attempting to fetch #{ds} (#{language}) from #{metadata[ds][language]["url"]}")
			Net::HTTP.start(host, :read_timeout => 10) do |http|
				resp = http.get(path)
				log("Got RESPONSE CODE #{resp.code} and CONTENT-TYPE #{resp['Content-Type']}")
				if resp.code == 404 || resp['Content-Type'] == 'text/html' || resp['Content-Type'] == 'text/plain'
					path.gsub!(/\/GEN\//,'/PRO/')
						log("FAILED fetching #{ds} (#{language}) from #{metadata[ds][language]["url"]}")
						#err("FAILED fetching #{ds} (#{language}) from #{metadata[ds][language]["url"]} (Response: #{resp.code})")
						log("Attempting alternate #{ds} (#{language}) from http://#{host}/#{path}")
					Net::HTTP.start(host, :read_timeout => 10) do |alt|
						altresp = alt.get(path)
						log("Got RESPONSE CODE #{altresp.code} and CONTENT-TYPE #{altresp['Content-Type']}")
						if altresp.code == 404 || altresp['Content-Type'] == 'text/html' || altresp['Content-Type'] == 'text/plain'
							err("PERMANENT FAILURE fetching #{ds} (#{language}) from http://#{host}/#{path} (Response: #{altresp.code})")
						else
							content_type = altresp['Content-Type']
							file_mimetype = MimeMagic.by_magic(altresp.body)
							if content_type == file_mimetype
								mimetype = content_type
							else
								mimetype = file_mimetype
							end
							extension = getext(mimetype)
							filename = "#{dsfn}_#{lxcode(language)}.#{extension}"
							outfile = "#{out_dir}/#{dsfn}_#{lxcode(language)}.#{extension}"
							open(outfile, "w+") do |f|
								f.write(altresp.body)
							end
							# Let's try to get the number of pages, eh?
							if File.file?(outfile) && File.size(outfile) > 512
								if mimetype == 'application/pdf'
									page_count_string = PDF::Reader.new(outfile).page_count.to_s + " page(s)"
								else	
									page_count_string = ""
								end
								contents << { :filename => filename, :language => language, :page_count => page_count_string, :mimetype => mimetype, :bitstream_order => bitstream_order}
								#File.open("#{out_dir}/contents", "a+") do |contents|
								#	contents.puts "#{outfile} bundle:ORIGINAL \"#{language} version #{page_count_string} #{mimetype} \""
								#end
								log("SUCCESSFULLY fetched #{ds} (#{language}) from http://#{host}/#{path}")
							else
								err("PERMANENT FAILURE fetching #{ds} (#{language}) from http://#{host}/#{path} (File likely corrupt or missing)")
							end
						end
					end
				else
					content_type = resp['Content-Type']
					#Since we can't trust this to be absolutely accurate, we have to do a separate test of the file. 
					file_mimetype = MimeMagic.by_magic(resp.body)
					if content_type == file_mimetype
						mimetype = content_type
					else
						mimetype = file_mimetype
					end
					extension = getext(mimetype)
					filename = "#{dsfn}_#{lxcode(language)}.#{extension}"
					outfile = "#{out_dir}/#{dsfn}_#{lxcode(language)}.#{extension}"
					open(outfile, "w+") do |f|
						f.write(resp.body)
					end
					# Let's try to get the number of pages, eh?
					if File.file?(outfile) && File.size(outfile) > 512
						if mimetype == 'application/pdf'
							page_count_string = PDF::Reader.new(outfile).page_count.to_s + " page(s)"
						else	
							page_count_string = ""
						end
						contents << { :filename => filename, :language => language, :page_count => page_count_string, :mimetype => mimetype, :bitstream_order => bitstream_order}
						#File.open("#{out_dir}/contents", "a+") do |contents|
						#	contents.puts "#{outfile} bundle:ORIGINAL \"#{language} version #{page_count_string} #{mimetype} \""
						#end
						log("SUCCESSFULLY fetched #{ds} (#{language}) from #{metadata[ds][language]["url"]}")
					else
						log("PERMANENT FAILURE fetching #{ds} (#{language}) from #{metadata[ds][language]["url"]} (File likely corrupt or missing)")
					end
				end
			end
		end
		#Sort the contents file in bitstream_order
		contents.sort!{|c1,c2| c1[:bitstream_order] <=> c2[:bitstream_order]}
		#Write contents file
		File.open("#{out_dir}/contents", "w+") do |file|
			contents.each do |c|
				file.puts "#{c[:filename]} bundle:ORIGINAL \"#{c[:language]} version #{c[:page_count]} #{c[:mimetype]}\""
			end
		end
	end
end

# Steps
# Read ARGV command line options and set variables accordingly
# 1: Get the latest slotted csv from AWS (TODO: determine best format, such as yyymmdd for designating latest csv)
# 2: Parse slotted csv to extract symbol, job number for each language, title, agenda item number, and agenda sub-item number
# 3: Use job numbers to generate ODS URL for retrieval.
# 4: Build package XMLs: dublin_core.xml and metadata_undr.xml
# 5: Build contents and download files
# 6: Repeat for non-slotted
steps = Array.new

opts = Trollop::options do
	banner <<-EOS
import_from_dgacm.rb processes DGCAM-originated metadata files (CSV format) and creates DSpace Simple Archive Packages from them.

Usage: 
	import_from_dgacm.rb [options] <filename>

where options are: 
EOS

	opt :remote, "Get the file from the remote S3 bucket.  Requires a credentials file"
	opt :get_latest, "Instead of specifying a filename, tell the script to get the latest available file.", :default => true
	opt :type, "The type of file being processed, slotted or non-slotted.", :type => String
	opt :local, "Get the file from a local file system."
	opt :credentials, "Path to a credentials file for S3.", :type => String
	opt :filename, "Filename containing metadata to parse.  Required if specifying a local file.", :type => String
end
Trollop::die :credentials, "<file> must be supplied" unless opts[:credentials] if opts[:remote]
Trollop::die :credentials, "<file> must exist" unless File.exist?(opts[:credentials]) if opts[:remote] && opts[:credentials]
Trollop::die :type, "is required unless specifying a filename.  Can be 'slotted' or 'non-slotted'." unless opts[:type] if opts[:get_latest]
Trollop::die :filename, "is required" if opts[:local]
Trollop::die :filename, "must exist" unless File.exist?(opts[:filename]) if opts[:local] && opts[:filename]

#Standard option 1a: remote with credentials, get latest slotted file
if opts[:remote] && opts[:credentials] && opts[:get_latest] && opts[:type] == 'slotted'
	
end

#Standard option 1b: remote with credentials, get latest non-slotted file
if opts[:remote] && opts[:credentials] && opts[:get_latest] && opts[:type] == 'non-slotted'
	
end

#Standard option 2: remote with credentials, get specific file (doesn't matter slotted vs non-slotted)
if opts[:remote] && opts[:credentials] && opts[:filename]
end

#Standard option3: local specific file (best for processing errors)
if opts[:remote] && opts[:credentials] && opts[:filename] && opts[:type] == 'non-slotted'
end

# Procedural logic
#log("Beginning DGACM packaging")
#slotted = get_latest_csv(access_key_id, secret_access_key, 'slotted')
#log("Processing entries from #{slotted}")
#metadata = parse_csv(slotted) or abort "Unable to read #{slotted} for some reason.  Check that the file exists and you have permission to read it."
#log("Found #{metadata.length} entries.")
#package(metadata) or abort "Something went wrong with packaging..."
#non_slotted = get_latest_csv(access_key_id, secret_access_key, 'non-slotted')
#log("Processing entries from #{non_slotted}")
#metadata = parse_csv(non_slotted) or abort "Unable to read #{non_slotted} for some reason.  Check that the file exists and you have permission to read it."
#log("Found #{metadata.length} entries.")
#package(metadata) or abort "Something went wrong with packaging..."

#Time to deal with errors with the 
#log("DGACM Packaging complete")
