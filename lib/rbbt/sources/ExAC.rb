require 'rbbt-util'
require 'rbbt/util/open'
require 'rbbt/resource'
require 'rbbt/persist'
require 'net/ftp'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))

module ExAC
  extend Resource
  self.subdir = "share/databases/ExAC"

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  URL = "ftp://ftp.broadinstitute.org/pub/ExAC_release/release1/ExAC.r1.sites.vep.vcf.gz"

  ExAC.claim ExAC["All.vcf.gz"], :url, URL

  def self.vcf_database(vcf_file, options = {})
    options = Misc.add_defaults options, :persist => true, :type => :list, :engine => 'BDB'
    db = Persist.persist_tsv("VCF Database", vcf_file, {}, options) do |data|
      Workflow.require_workflow "Sequence"
      parser = TSV::Parser.new Sequence::VCF.open_stream(Open.open(vcf_file, :nocache => true), false, false, true)

      data.fields = parser.fields
      data.key_field = parser.key_field
      data.serializer = :list

      TSV.traverse parser do |mutation,values|
        chr, position, alt = mutation.split(":")
        mutations = alt.split(",").collect{|mut|
          [chr, position, mut] * ":"
        }

        mutations.each do |mutation|
          data[mutation] = values
        end
      end

      data
    end
  end

  def self.database
    @@database ||= begin
                     vcf_database(ExAC["All.vcf.gz"])
                   end
  end
end
