#!/usr/bin/ruby
# encoding: UTF-8

# This script should be run with ruby version > 1.9.3 and the following libraries and gems

require 'rubygems'
require 'smarter_csv'	#External ruby gem.  gem install smarter_csv to install it
require 'net/http'
require 'aws/s3'	#External ruby gem.  gem install aws-s3 to install it
require 'date'
require 'pdf-reader'	#External ruby gem.  gem install pdf-reader to install it
require 'trollop'	#External ruby gem.  gem install trollop to install it
require 'rexml/document'
require 'json'

# Utility functions
def log(message)
	logfile = "logs/dgacm_to_json.log"
	if !File.exists?('logs')
		Dir.mkdir('logs') or abort "Unable to create logs directory.  Make sure you have permission to write to it and try again."
	end
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def s3_latest_csv(credentials,prefix)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	files = AWS::S3::Bucket.find(bucket).objects( :prefix => prefix )
	epoch_time = DateTime.parse('1970-01-01T00:00:00+00:00')
	latest_time = epoch_time
	latest_file = nil

	files.each do |file|
		if file.key.downcase =~ /^#{prefix.downcase}\/dhl\-edoc/
			mtime = DateTime.parse("#{file.about["last-modified"]}")
			if mtime > latest_time
				latest_file = file.key
			end
		end
	end
	if latest_file
		p latest_file
		File.open(latest_file.gsub(/#{prefix}\//,''), 'w') do |f|
			AWS::S3::S3Object.stream(latest_file, bucket) do |chunk|
				f.write chunk
			end
		end
		return latest_file.gsub(/#{prefix}\//,'')
	else
		return nil
	end
end

def s3_find_files(credentials, prefix, match_pattern)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	match_files = Array.new
	files = AWS::S3::Bucket.find(bucket).objects( :prefix => prefix )
	files.each do |file|
		if file.key.downcase =~ /#{match_pattern}/
			match_files << file.key
			p file.key
		end
	end
	if match_files.length > 0 
		return match_files
	else
		return nil
	end
end

def s3_exists?(credentials, filename)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	if AWS::S3::S3Object.exists?(filename,bucket)
		return true
	else
		return false
	end
end

def s3_fetch(credentials, filename)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	if AWS::S3::S3Object.exists?("#{filename}",bucket)
		return AWS::S3::S3Object.value filename, bucket
	else
		return nil
	end
end

def s3_write(credentials, filename, data)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	AWS::S3::S3Object.store(filename, data, bucket)
end

def s3_move(credentials, filename, destination)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	unless AWS::S3::S3Object.exists?(destination,bucket)
		p "-------moving #{filename} to #{destination}"
		AWS::S3::S3Object.rename(filename, destination, bucket)
	end
	#AWS::S3::S3Object.rename(latest_file, latest_file.gsub(/Drop\//, 'Drop/processed/'), 'undhl-dgacm')
end

def s3_delete(credentials, filename)
	bucket = credentials[0]
	access_key_id = credentials[1]
	secret_access_key = credentials[2]
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	if AWS::S3::S3Object.exists?("#{filename}",bucket)
		AWS::S3::S3Object.delete filename, bucket
		return 0
	else
		return nil
	end
end

def parse_csv(csv_file)
	debug = 0		#REALLY avoid this unless you need it.  Keep it at 0!!!  You've been warned.

	transitional = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	metadata = Hash.new
	csv_data = SmarterCSV.process(csv_file, { :col_sep => "\t", :file_encoding => 'utf-8', :verbose => true })
	csv_data.each do |row|
		if row[:symbol] =~ /\s\s/
			docsymbol = row[:symbol].split(/\s+/)[0]
		else
			docsymbol = row[:symbol]
		end
		case row[:lang]
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
			else
				language = "Other"
		end
		transitional[docsymbol][language] = row
	end
	metadata = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	docsymbols = transitional.keys
	docsymbols.each do |ds|
		agenda = ''
		languages = transitional[ds].keys
		if transitional[ds].key?("English") 
			#if transitional[ds]["English"][:distribution].to_s.strip.downcase == "GENERAL".downcase
				if 	transitional[ds]["English"][:agen_num]
					agenda = transitional[ds]["English"][:agen_num].to_s
				end
				metadata[ds] = {
				"doc_num" => transitional[ds]["English"][:doc_num].to_s.strip,
				"title" => transitional[ds]["English"][:title].to_s.strip,
				"symbols" => transitional[ds]["English"][:symbol].to_s.strip.split(/\s+/),
				"distribution" => transitional[ds]["English"][:distribution].to_s.strip,
				"isbn" => transitional[ds]["English"][:isbn].to_s.strip,
				"issn" => transitional[ds]["English"][:issn].to_s.strip,
				"cr_sales_num" => transitional[ds]["English"][:cr_sales_num].to_s.strip,
				"issued_date" => transitional[ds]["English"][:publication_date].to_s,
				"slot_num" => transitional[ds]["English"][:slot_num].to_s.strip,
				"agen_num" => agenda, 
				"languages" =>  languages }
			
				#Get the non-unique stuff next
				languages.each do |language|
					jn = transitional[ds][language][:job_num].strip.gsub(/NY-J-/,'N').gsub(/-/,'').gsub(/LP/,'').gsub(/\*/,'').gsub(/\/1/,'').gsub(/X/,'')
					metadata[ds][language] = { 
						"job_num" => transitional[ds][language][:job_num].to_s.strip,
						"filename" => jn + ".pdf",
						"language" => language
					}
				end
			#end
		else
			# There's something really wrong with this package??  Not sure what to do with it, but don't want to delete it outright.  Let's pass it back with an extra field called missing_english.
			tkey = transitional[ds].keys.first
			#p transitional[ds][tkey][:agen_num]
			if transitional[ds][tkey][:agen_num]
				agenda = transitional[ds][tkey][:agen_num].to_s
			end
			metadata[ds] = {
				"doc_num" => transitional[ds][tkey][:doc_num].to_s.strip,
				"title" => transitional[ds][tkey][:title].to_s.strip,
				"symbols" => transitional[ds][tkey][:symbol].to_s.strip.split(/\s+/),
				"distribution" => transitional[ds][tkey][:distribution].to_s.strip,
				"isbn" => transitional[ds][tkey][:isbn].to_s.strip,
				"issn" => transitional[ds][tkey][:issn].to_s.strip,
				"cr_sales_num" => transitional[ds][tkey][:cr_sales_num].to_s.strip,
				"issued_date" => transitional[ds][tkey][:publication_date].to_s,
				"slot_num" => transitional[ds][tkey][:slot_num].to_s.strip,
				"agen_num" => agenda, 
				"languages" =>  languages,
				"missing_english" => true }
			
			#Get the non-unique stuff next
			languages.each do |language|
				jn = transitional[ds][language][:job_num].strip.gsub(/NY-J-/,'N').gsub(/-/,'').gsub(/LP/,'').gsub(/\*/,'').gsub(/\/1/,'').gsub(/X/,'')
				metadata[ds][language] = { 
					"job_num" => transitional[ds][language][:job_num].to_s.strip,
					"filename" => jn + ".pdf",
					"language" => language
				}
			end
		end
	end
	if debug == 1
		metadata.each do |m|
			log(m) 
		end
	end
	return metadata
end

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
dgacm_to_json.rb processes DGCAM-originated metadata files (CSV format) and writes each metadata set to individual JSON files.  The default option is to process the newest CSV found.  Successfully processed CSVs are then moved (renamed) in S3 to a processed folder so they won't be picked up again.  An error reconciliation routine is also provided; it traverses the S3 file system looking for .error files and attempting to complete the packages.  

Note: The credentials file should be in the format bucket::access_key_id::secret_access_key.  Note the separator '::'

Usage: 
	dgacm_to_json.rb [options]

where options are: 
EOS

	opt :s3_credentials, "Path to a credentials file for S3.", :type => String
	opt :reconcile_errors, "Seek out *.error files and attempt to complete the packages."
	opt :print_summary, "Print a summary of the process results."
end
Trollop::die :s3_credentials, "<file> must be supplied" unless opts[:s3_credentials] 
Trollop::die :s3_credentials, "<file> must exist" unless File.exist?(opts[:s3_credentials]) && opts[:s3_credentials]

# This file has been verified to exist.  It doesn't mean it contains actual credentials, though.
CREDS = File.read(opts[:s3_credentials]).split('::')

if opts[:reconcile_errors]
	# Need to get a list of files in #{bucket}/Drop/packages that have .error in the filename.  With that, we can process the error files one by one.  
	# If we complete the package, delete the .error file.  Otherwise, either update the file with any new information learned, or just leave the file alone.
	log("Performing error reconciliation.")
	package_complete = false
	error_files = s3_find_files(CREDS,'Drop/packages','\.error')
	if error_files
		puts "Found #{error_files.size} incomplete or empty packages."
		log("Found #{error_files.size} incomplete or empty packages.")
		error_files.each do |file|
			new_json = Hash.new
			still_missing_files = Array.new
			json = JSON.parse(s3_fetch(CREDS,file)) or abort "Unable to read file #{file} from S3."
			still_missing_count = json["missing_files"].size
			puts "File #{file} references #{json["missing_files"].size} missing files."
			log("File #{file} references #{json["missing_files"].size} missing files.")
			json["missing_files"].each do |missing|
				#p missing["name"]
				s3file = s3_find_files(CREDS,'Drop/',missing["name"])
				if s3file
					# We are one file closer to completing a package.
					still_missing_count = still_missing_count - 1
					s3_move(CREDS,s3file,json["package"])
				else
					# Hmm  do anything here?
					still_missing_files << { "name" => missing["name"], "language" => missing["language"] }
					#p still_missing_files
				end
			end
			if still_missing_count > 0
				# Package is still incomplete.  If still_missing_count == original missing count, then nothing new was found.
				# Otherwise we can remove the old error file and write a new error file in its place, minus the found files.
				if still_missing_count < json["missing_files"].size
					#remove old file
					s3_delete(CREDS,file) or abort "Unable to delete file #{file}."
					#start making a new one
					new_json = JSON.generate({ "package" => json["package"], "missing_files" => still_missing_files})
					s3_write(CREDS,file,new_json)
				#else do nothing
				end
				puts "#{still_missing_count} files are still missing for this package."
				log("#{still_missing_count} files are still missing for this package.")
			else
				# Package is now complete.
				s3_delete(CREDS,file) or abort "Unable to delete file #{file}."
				puts "Package #{json["package"]} is now marked as complete."
				log("Package #{json["package"]} is now marked as complete.")
			end
		end
	end
else
	# This is the default action
	log('Checking for unprocessed metadata files')
	metadata_file = s3_latest_csv(CREDS,'Drop')
	if metadata_file
		log("Found unprocessed metadata file #{metadata_file}.  Parsing it now.")
		metadata_file_date = metadata_file[13..16] + '-' + metadata_file[17..18] + '-' + metadata_file[19..20]
		file_prefix = "Drop/packages/#{metadata_file_date}"
		metadata = parse_csv(metadata_file)
		docsymbols = metadata.keys
		docsymbols.each do |ds|
			incompletes = Array.new
			dsfs = ds.gsub(/\//,'_').gsub(/\s+/,'_')
			s3_write(CREDS,"#{file_prefix}/#{dsfs}/#{dsfs}.json",JSON.generate(metadata[ds]))
			if metadata[ds]["missing_english"]
				incompletes << { "name" => "missing_file", "language" => "English" }
			end
			metadata[ds]["languages"].each do |language|
				#remote_file = s3_find_files(CREDS,'Drop',metadata[ds][language]["filename"])
				if s3_exists?(CREDS,"Drop/#{metadata[ds][language]["filename"]}")
					#p "Moving Drop/#{metadata[ds][language]["filename"]} to #{file_prefix}/#{dsfs}/#{metadata[ds][language]["filename"]}"
					s3_move(CREDS,"Drop/#{metadata[ds][language]["filename"]}","#{file_prefix}/#{dsfs}/#{metadata[ds][language]["filename"]}")
				else
					incompletes << { "name" => metadata[ds][language]["filename"], "language" => language }
				end
			end
			if incompletes.size > 0
				error_data = { "package" => "#{file_prefix}/#{dsfs}", "missing_files" => incompletes }
				s3_write(CREDS, "#{file_prefix}/#{dsfs}/#{dsfs}.error", JSON.generate(error_data))
			end
		end
		# Finally we can move the metadata file into the processed folder.
		s3_move(CREDS,"Drop/#{metadata_file}","Drop/processed/#{metadata_file}")
	else
		log("No new metadata files found")
	end
end