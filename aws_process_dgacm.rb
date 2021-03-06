#!/bin/env ruby
# encoding: UTF-8

require 'rubygems'
require 'aws-sdk'
require 'trollop'
require 'json'
require 'active_support/core_ext'
require 'smarter_csv'
require 'pdf-reader'
require 'rexml/document'

require_relative 'lib/functions.rb'
require_relative 'lib/constants.rb'

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
        banner <<-EOS
  aws_process_dgacm.rb does all of the following tasks, depending on which 
options and arguments are provided when the script is invoked.

1.  It scans the given S3 bucket for new DGACM-provided metadata files, makes 
    package folders for them, and attempts to locate PDFs that match with the 
    given metadata.  
2.  Before creating a new folder, it will attempt to determine whether the 
    metadata has already been processed by searching DynamoDB.
3.  It creates DynamoDB entries for newly encountered document symbols to allow
    for more efficient querying of 
4.  It can search for and attempt to re-process incomplete packages.
5.  It can parse PDFs to extract basic metadata in cases where a file was 
    provided without metadata.  This it uses either to locate any existing 
    package to which it might belong, or to make a separate archive of packages 
    and new database entries referencing them.
6.  Finally, it can output a report with varying granularity of what packages 
    are here, what appears to be incomplete or missing metadata, and so on.

Note: This script requires a set of AWS credentials that has read/write access 
to both S3 and DynamoDB.  Credentials are read from an external JSON-formatted 
file containing "accessKeyId" and "secretAccessKey".

Try not to combine too many options at once.  Each option set serves different
purposes, and if one fails, you will want to be able to track down the source
of the failure.  This is easier to do if you haven't chained together too many
option calls.  In case you need other reasons to avoid chaining options
together, keep in mind that most S3 functions will begin at the specified
prefixes and usually won't move up the folder hierarchy.  Thus your invocation
is likely to fail if you haven't thought this out properly.

Typical usage scenarios look like this (full usage notes are below):

1.  Process the latest metadata file available on the S3 drive in the specified
    bucket/prefix combination

    aws_process_dgacm --aws-credentials <file> --s3-bucket <bucket>
      --s3-root-prefix <prefix> --s3-package-prefix <prefix>
      --dynamo-table <table> 

2.  Process a specific metadata file available either locally or somewhere in 
    the specified prefix path on S3.

    aws_process_dgacm --aws-credentials <file> --s3-bucket <bucket>
      --s3-root-prefix <prefix> --s3-package-prefix <prefix>
      --dynamo-table <table> --specific-file <file>

3.  Reprocess incomplete packages as listed in DynamoDB

    aws_process_dgacm --aws-credentials <file> --s3-bucket <bucket>
      --s3-root-prefix <prefix> --s3-package-prefix <prefix>
      --dynamo-table <table> --reprocess

4.  Parse undescribed PDFs in S3 and try to do something with them.

    aws_process_dgacm --aws-credentials <file> --s3-bucket <bucket>
      --s3-root-prefix <prefix> --s3-package-prefix <prefix>
      --dynamo-table <table> --parse-pdfs

5.  Generate a report for November 2013

    aws_process_dgacm --aws-credentials <file> --s3-bucket <bucket>
      --s3-root-prefix <prefix> --s3-package-prefix <prefix>
      --dynamo-table <table> --generate-report --report-start 2013-11-01
      --report-end 2013-11-30

Usage:
  aws_process_dgacm.rb [options]

where options are:
EOS

  opt :aws_credentials, "Path to a JSON-formatted credentials file that can read and write to both S3 and DynamoDB.  Required.", :type => String
  opt :s3_bucket, "Name of the S3 Bucket you wish to use.  Required.", :type => String
  opt :s3_root_prefix, "Path of the folder you wish to scan.", :type => String
  opt :s3_package_prefix, "Name of the S3 subfolder you want packages to reside in.", :type => String
  opt :dynamo_table, "Name of the DynamoDB table you wish to use.  Required.", :type => String
  opt :make_packages, "Whether or not to make packages from existing metadata and files."
  opt :reprocess, "Whether or not to reprocess incomplete packages detected by the script."
  opt :latest_file, "Process the latest CSV metadata file in S3."
  opt :specific_file, "Name of a specific file to process.  Checks locally first before searching S3.", :type => String
  opt :parse_pdfs, "Whether to parse PDFs for extractable metadata."
  opt :generate_report, "Whether to generate a report showing which packages are complete and which incomplete."
  opt :report_start, "YYYY-MM-DD formatted date to start reporting.  Default is month to date.", :type => String
  opt :report_end, "YYYY-MM-DD formatted date to end reporting.  Default is today.", :type => String
  conflicts :latest_file, :make_packages
  conflicts :latest_file, :reprocess
  conflicts :latest_file, :specific_file
  conflicts :latest_file, :parse_pdfs
  conflicts :latest_file, :generate_report
  conflicts :specific_file, :make_packages
  conflicts :specific_file, :reprocess
  conflicts :specific_file, :parse_pdfs
  conflicts :specific_file, :generate_report
  conflicts :reprocess, :make_packages
  conflicts :reprocess, :parse_pdfs
  conflicts :reprocess, :generate_report
  conflicts :parse_pdfs, :make_packages
  conflicts :parse_pdfs, :generate_report
  conflicts :generate_report, :make_packages
end
Trollop::die :aws_credentials, "<file> must be supplied" unless opts[:aws_credentials]
Trollop::die :aws_credentials, "<file> must exist.  Check your path and try again" unless File.exists?(opts[:aws_credentials])
Trollop::die :s3_bucket, "<bucket> is a required argument" unless opts[:s3_bucket]
Trollop::die :dynamo_table, "<table> is a required argument" unless opts[:dynamo_table]
Trollop::die :report_start, "<date> must be in the format YYYY-MM-DD, e.g., 2013-11-01" unless opts[:report_start] =~ /(\d+)-(\d+)-(\d+)/ if opts[:report_start]
Trollop::die :report_end, "<date> must be in the format YYYY-MM-DD, e.g., 2013-11-30" unless opts[:report_end] =~ /(\d+)-(\d+)-(\d+)/ if opts[:report_end]
Trollop::die :s3_package_prefix, "<prefix> must be supplied if choosing the --make-packages command" unless opts[:s3_package_prefix] if opts[:make_packages]


#First let's create a temp folder
if !File.exists?('tmp')
  Dir.mkdir('tmp') or abort "Unable to create a tmp directory.  Check your permissions and try again."
end

AWSCREDS = JSON.parse(File.read(opts[:aws_credentials]))
AWS.config( access_key_id: AWSCREDS["accessKeyId"], secret_access_key: AWSCREDS["secretAccessKey"] )
ddb = AWS::DynamoDB.new(
  :access_key_id => AWSCREDS["accessKeyId"],
  :secret_access_key => AWSCREDS["secretAccessKey"]
)
db_table = ddb.tables[opts[:dynamo_table]]
db_table.hash_key = ["JobNumber", :string]
db_client = AWS::DynamoDB::Client.new(:api_version => '2012-08-10')
s3 = AWS::S3.new(
  :access_key_id => AWSCREDS["accessKeyId"],
  :secret_access_key => AWSCREDS["secretAccessKey"]
)
s3_bucket = opts[:s3_bucket]
s3_root = opts[:s3_root_prefix]

#Next let's process the options
if opts[:reprocess]
  #Do reprocess
  db_table.items.each do |item|
    puts item.hash_value
  end
end
if opts[:specific_file]
  #Check if file exists locally
  #If not, check in each prefix node on S3
  #Finally, check the /Drop/processed folder on S3
  #Trollop::die if it's not found in any location
  if File.exists?(opts[:specific_file])
    #Exists locally
    puts "Found #{opts[:specific_file]} to process."
    fname = opts[:specific_file]
  elsif File.exists?("tmp/#{opts[:specific_file]}")
    #Exists locally
    puts "Found tmp/#{opts[:specific_file]} to process."
    fname = "tmp/#{opts[:specific_file]}"
  elsif s3.buckets[s3_bucket].objects["#{opts[:s3_root_prefix]}/#{opts[:specific_file]}"].exists?
    puts "Found S3:/#{opts[:s3_root_prefix]}/#{opts[:specific_file]} to process."
    fname = "tmp/#{opts[:specific_file]}"
  elsif s3.buckets[s3_bucket].objects["#{opts[:s3_root_prefix]}/processed/#{opts[:specific_file]}"].exists?
    puts "Found S3:/#{opts[:s3_root_prefix]}/processed/#{opts[:specific_file]} to process."
    fname = "tmp/#{opts[:specific_file]}"
  end
end
if opts[:latest_file]
  #Do process the latest CSV in S3
  metadata_file = s3_latest_csv(s3,s3_bucket,s3_root,"dhl\-edoc")
  if metadata_file 
    puts "Processing entries from #{metadata_file.key}"
    fname = "tmp/#{metadata_file.key.gsub(/Drop\//,'')}"
    File.open(fname, 'wb') do |file|
      s3.buckets[s3_bucket].objects[metadata_file.key].read do |chunk|
        file.write(chunk)
      end
    end
    if File.size(fname) > 110 
      items_array = parse_csv(s3,s3_bucket,fname)
      if items_array
        items_array.each do |item_hash|
          item = db_table.items.create( item_hash )
        end
      else
        Trollop::die "<file> could not be parsed."
      end
    end
    old_key = metadata_file.key
    new_key = old_key.gsub(/Drop\//,'Drop/processed/')
    puts "Moving #{old_key} to #{new_key}"
    metadata_file.move_to(new_key)
  else
    puts "No new metadata files found to process."
  end
end
if opts[:parse_pdfs]
  #Do parse PDFs at prefix root
  $stdout.sync = true
  #Need a temp file
  puts "Searching for undescribed PDFs"
  print "["
  files = s3_list_files_in_folder(s3,s3_bucket,s3_root,'n\d{7}.pdf')
  files.each do |file|
    item_hash = Hash.new
    fname = file.key.gsub(/Drop/,'tmp')
    File.open(fname, 'wb') do |f|
      s3.buckets[s3_bucket].objects[file.key].read do |chunk|
        f.write(chunk)
      end
    end
    reader = PDF::Reader.new(fname)
    info = reader.info
    # Is it possible to have more than one entry for Symbol, such as Symbol2???  Probably...
    bare_fn = fname.gsub(/tmp\//,'')
    jobnumber = "NY-J-#{bare_fn[1..2]}-#{bare_fn[3..7]}-"
    jobnumber1 = "NY-J-#{bare_fn[1..2]}-#{bare_fn[3..7]}-*"
    if db_table.items[jobnumber].exists?
      #puts "Found a match! #{jobnumber} is already in the database."
      print "."
      File.delete(fname)
      next
    elsif db_table.items[jobnumber1].exists?
      #puts "Found a match! #{jobnumber1} is already in the database."
      print "."
      File.delete(fname)
      next
    else
      #puts "Neither #{jobnumber} nor #{jobnumber1} were found in the database."
      item_hash['JobNumber'] = jobnumber
      if info[:Symbol1]
        docsymbol = info[:Symbol1]
        #puts "\tWill add it with docsymbol #{docsymbol}"
        item_hash['DocumentSymbol'] = docsymbol
      else
        #puts "\tAdditionally, unable to determine the document symbol for this file."
        print "(!#{bare_fn})"
      end
    end
    if info[:CreationDate]
      item_hash['PublicationDate'] = info[:CreationDate].gsub(/D\:/,'')[0..7].to_i
    end
    # Parse for language
    if info.to_s.downcase =~ /atpu/
      item_hash["Language"] = "A"
    elsif info.to_s.downcase =~ /ctpu/
      item_hash["Language"] = "C"
    elsif info.to_s.downcase =~ /etpu/
      item_hash["Language"] = "E"
    elsif info.to_s.downcase =~ /ftpu/
      item_hash["Language"] = "F"
    elsif info.to_s.downcase =~ /rtpu/
      item_hash["Language"] = "R"
    elsif info.to_s.downcase =~ /stpu/
      item_hash["Language"] = "S"
    else
      item_hash["Language"] = "O"
    end 
    item_hash["Filename"] = file.key
    item_hash["FileStatus"] = "Found"
    #We're done with it.  Probably.
    File.delete(fname)
    #puts item_hash
    print "(+#{bare_fn})"
    item = db_table.items.create( item_hash )
  end
  print "]"
end
if opts[:generate_report]
  if opts[:report_start]
    #Format is validated in the options parsing above
    report_start = opts[:report_start]
  else
    report_start = Date.today.at_beginning_of_month
  end
  if opts[:report_end]
    #Format is validated in the options parsing above
    report_end = opts[:report_end]
  else
    report_end = Time.now.localtime.strftime("%Y-%m-%d")
  end
  #Do generate report
end
if opts[:make_packages]
  $stdout.sync = true
  complete_list = Array.new
  #Step 1: Read the values in S3:/:s3_root_prefix/:s3_package_prefix/complete.list to get the docsymbols to exclude
  if s3.buckets[s3_bucket].objects["#{opts[:s3_root_prefix]}/#{opts[:s3_package_prefix]}/complete.list"].exists?
    puts "Reading S3://#{opts[:s3_root_prefix]}/#{opts[:s3_package_prefix]}/complete.list..."
    file = s3.buckets[s3_bucket].objects["#{opts[:s3_root_prefix]}/#{opts[:s3_package_prefix]}/complete.list"]
    complete_list = s3.buckets[s3_bucket].objects[file.key].read.to_a
  else
    puts "No database file to parse, will create one after this run."
  end
  #Step 2: Get a unique list of DocumentSymbols from the db_table, but only if they aren't already in complete.list
  puts "Finding unprocessed complete packages."
  syms = Array.new
  print "["
  db_table.items.select('DocumentSymbol').each do |i|
    unless syms.include?(i.attributes['DocumentSymbol'])
      unless complete_list.include?(i.attributes['DocumentSymbol'])
        print "|+#{i.attributes['DocumentSymbol']}|"
        syms << i.attributes['DocumentSymbol']
      else
        print "."
      end
    end
  end
  puts "]"
  #Step 3: Attempt to classify remaining symbols in terms of completeness.  Those missing files are certainly not complete, 
  #but there is no guarantee that those with all files present are complete.  If we receive files later that are indicated 
  #for already-completed packages, we should generate a replacement package.
  syms.each do |sym|
    if sym == nil
      next
    end
    #puts sym
    files = db_client.query( {  
      table_name: "#{db_table.name}", 
      index_name: "DocumentSymbol-FileStatus-index",
      select: 'ALL_PROJECTED_ATTRIBUTES',
      key_conditions: {
        'DocumentSymbol' => {
          comparison_operator: 'EQ',
          attribute_value_list: [
            { 's' => "#{sym}" }
          ]
        }
      } 
    } )
    if files[:count] < 1
      next
    end
    package_status = 'Complete'
    docsymbol = nil
    files[:member].each do |file|
      #p file["FileStatus"][:s]
      if file["DocumentSymbol"][:s] && file["DocumentSymbol"][:s].length > 1 
        docsymbol = file["DocumentSymbol"][:s]
        if !file["FileStatus"] || !file["FileStatus"][:s]
          package_status = "Incomplete"
        end
        if file["FileStatus"][:s] == "Missing"
          package_status = "Incomplete"
        end
      else
        package_status = 'Incomplete'
      end
    end
    if package_status == 'Complete'
      complete_list << docsymbol.chomp
    end
  end 
  if complete_list
    # Step 4: Process entries from complete.list, arranging packages according to document symbol.  When files are moved,
    # we will certainly need to update the record in DynamoDB
    puts "Making packages"
    complete_list.each do |docsymbol|
      metadata = nil
      metadata_member = nil
      out_dir = "#{opts[:s3_root_prefix]}/#{opts[:s3_package_prefix]}/#{docsymbol.gsub(/\//,'_').gsub(/\s+/,'_')}"
      dublin_core_xml = "#{out_dir}/dublin_core.xml"
      if s3.buckets[s3_bucket].objects[dublin_core_xml].exists?
        puts "Resuming previous run."
        next
      end
      dcxml = nil
      metadata_undr_xml = "#{out_dir}/metadata_undr.xml"
      muxml = nil
      contents_file = "#{out_dir}/contents"
      contents = Array.new
      languages = Array.new
      # Refactor idea: this would be more efficient if the query were done only once; this is the second query against DynamoDB...
      # Consider consolidation?
      files = db_client.query( {
        table_name: "#{db_table.name}",
        index_name: "DocumentSymbol-FileStatus-index",
        select: 'ALL_PROJECTED_ATTRIBUTES',
        key_conditions: {
          'DocumentSymbol' => {
            comparison_operator: 'EQ',
            attribute_value_list: [
              { 's' => "#{docsymbol}" }
            ]
          }
        }
      } )
      #By now we should be working only with a list of docysmbols for which all files that have been described have also been 
      #accounted for.  The metadata for each file under a docsymbol should be identical, so there *should* be no reason to care
      #which one we take the final metadata from. 
      files[:member].each do |file|
        chash = Hash.new
        jobnum = file["JobNumber"][:s]
        metadata = db_client.query( {
          table_name: "#{db_table.name}",
          select: 'ALL_ATTRIBUTES',
          key_conditions: {
            'JobNumber' => {
              comparison_operator: 'EQ',
              attribute_value_list: [
                { 's' => "#{jobnum}" }
              ]
            }
          }
        } )
        #puts metadata
    # Step 5: Make a dublin_core.xml file, a metadata_undr.xml file, and a contents file.
        metadata_member = metadata[:member].first
        language = metadata_member["Language"][:s]
        languages << language
        #puts language
        new_file_name = "#{docsymbol.gsub(/\//,'_').gsub(/\s+/,'_')}-#{language}.pdf"
        full_new_file_name = "#{out_dir}/#{new_file_name}"
        contents << "#{new_file_name}\tbundle:ORIGINAL"
        #Step 6: move the file on S3
        old_file_name = metadata_member["Filename"][:s]
        if old_file_name =~ /\_/
          next
        end
        #this might be dangerous
        if !s3.buckets[s3_bucket].objects[old_file_name].exists?
          # The logic here is that the file was previously moved for some reason.
          # I don't know if this is a good assumption, but there should have been 
          # numerous hurdles already overcome to establish that this filename was
          # found here at some point.
          next
        end

        puts "Moving #{old_file_name} to #{full_new_file_name}"
        s3_move_file(s3,s3_bucket,old_file_name,full_new_file_name)
        #Step 7: Update the db entry
        puts "Updating DynamoDB entry with the new location."
        db_update = db_client.update_item( {
          table_name: "#{db_table.name}",
          key: {
            "JobNumber" => {
              "s" => "#{jobnum}"
            }
          },
          attribute_updates: {
            "Filename" => {
              value: {
                "s" => "#{full_new_file_name}"
              },
              action: "PUT"
            }
          }
        } )
        #Trollop::die "Stopping here..."
      end
      dcxml = make_dublin_core_xml(metadata_member,languages)
      muxml = make_undr_xml(metadata_member)
      if contents
        puts "Writing S3://#{contents_file}"
        s3.buckets[s3_bucket].objects.create(contents_file,contents.join("\n"))
      end
      if dcxml
        puts "Writing S3://#{dublin_core_xml}"
        s3.buckets[s3_bucket].objects.create(dublin_core_xml,dcxml)
      end
      if muxml
        puts "Writing S3://#{metadata_undr_xml}"
        s3.buckets[s3_bucket].objects.create(metadata_undr_xml,muxml)
      end
    end

    # Step 8: Write out a new complete.list file with values of processed symbols
    s3.buckets[s3_bucket].objects['Drop/packages/complete.list'].write(complete_list.join("\n"))
  end
end
