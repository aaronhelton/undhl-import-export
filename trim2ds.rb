#!/bin/env ruby

require 'fileutils'
require 'rubygems'
require 'nokogiri'
require 'open-uri'

fonds_name = ''
series_name = ''
folder_name = ''
item_title = ''
item_date_issued = ''
item_type = 'Archival Item'
bitstream_path = ''
outdir = "package"

class MetadataSet
  attr_reader :id, :metadata, :schema

  def initialize(id, metadata, scheme)
    @id = id
    @metadata = metadata
    @schema = schema
  end

  def write(path, format)
    if !File.exists?(path)
      File.open(path, "w") do |f|
        if format == 'xml'
          f.puts(self.to_xml)
        elsif format == 'tab'
          f.puts(self.to_tab)
        end
      end
    end
  end

  def to_xml(*a)
    if @schema == 'contents' then return nil end
    xml = '<?xml version="1.0" encoding="UTF-8"?>'
    if @schema
      xml += '<dublin_core schema="' + @schema + '">'
    else
      xml += '<dublin_core>'
    end
    @metadata.each do |m|
      xml += '  <dcvalue element="' + m[:element] + '" qualifier="' + m[:qualifier]  + '">' + m[:value] + '</dcvalue>'
    end
    xml += '</dublin_core>'
    return xml
  end

  def to_tab(*a)
    tab = ''
    @metadata.each do |m|
      tab = "#{m[:name]}\t#{m[:desc]}"
    end
    return tab
  end
end

xmlfeed = Nokogiri::XML(open("ARMSmetadata.xml"))
fonds_name = xmlfeed.xpath("/TRIM/RECORD/Title").text
xmlfeed.xpath("/TRIM/RECORD/RECORD").each do |series|
  series_name = series.xpath("Title").text
  series.xpath("RECORD").each do |folder|
    folder_name = folder.xpath("Title").text
    folder.xpath("RECORD").each do |item|
      item_title = item.xpath("title").text
      item_date_issued = item.xpath("datecreated").text
      bitstream_path = "#{item.xpath("number").text}.pdf"
      item_id = "#{item.xpath("number").text}"
      if File.exists?(bitstream_path)
        dc_metadata = Array.new
        undr_metadata = Array.new
        contents_metadata = Array.new
        if !Dir.exists?(outdir)
          Dir.mkdir(outdir) or abort "Unable to create package directory"
        end
        if !Dir.exists?("#{outdir}/#{item_id}")
          Dir.mkdir("#{outdir}/#{item_id}")
        end
        dc_metadata << { :element => "type", :qualifier => "none", :value => "Archival Item" }
        dc_metadata << { :element => "title", :qualifier => "none", :value => item_title }
        dc_metadata << { :element => "date", :qualifier => "issued", :value => item_date_issued }
        undr_metadata << { :element => "cluster", :qualifier => "series", :value => "#{fonds_name}::#{series_name}::#{folder_name}" }
        contents_metadata << { :name => bitstream_path, :desc => "bundle:ORIGINAL" }

        d = MetadataSet.new(item_id, dc_metadata, nil)
        d.write("#{outdir}/#{item_id}/dublin_core.xml", "xml")
        u = MetadataSet.new(item_id, undr_metadata, "undr")
        u.write("#{outdir}/#{item_id}/metadata_undr.xml", "xml")
        c = MetadataSet.new(item_id, contents_metadata, "contents")
        c.write("#{outdir}/#{item_id}/contents", "tab")
        
        FileUtils.cp(bitstream_path, "#{outdir}/#{item_id}/#{bitstream_path}")
      end
    end
  end
end
