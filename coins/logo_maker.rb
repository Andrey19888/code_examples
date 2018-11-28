#
# This class does following work:
#   1) Downloads logo from CMC.
#   2) Uploads downloaded CMC logo to Amazon S3.
#   3) Updates column "aw_image_url" in table to follow to Amazon S3 file.

require 'open-uri'

module Coins
  class LogoMaker
    DOWNLOAD_DIR = Rails.root.join('tmp', 'downloads').freeze

    BUCKET = ENV.fetch('AWS_S3_BUCKET').freeze
    BUCKET_PATH = 'coins'.freeze

    def initialize(meta_id)
      @meta = CoinMeta.find(meta_id)
    end

    def perform
      downloaded_file = download_cmc_logo
      s3_public_url = upload_to_s3(downloaded_file)
      @meta.update(aw_image_url: s3_public_url)

    rescue OpenURI::HTTPError
      Rails.logger.tagged(self.class.name) do |logger|
        logger.warn("couldn't download CMC logo for meta ##{@meta.id} (#{@meta.cmc_slug}; #{@meta.cmc_logo_url})")
      end

    ensure
      if downloaded_file && File.exist?(downloaded_file)
        FileUtils.rm(downloaded_file)
      end
    end

    private

    def download_cmc_logo
      FileUtils.mkdir_p(DOWNLOAD_DIR)

      file_name = "#{self.class.name}.#{@meta.id}.#{Time.current.to_f}"
      file_path = File.join(DOWNLOAD_DIR, file_name)

      open(@meta.cmc_logo_url, 'rb') do |read_file|
        File.binwrite(file_path, read_file.read)
      end

      file_path
    end

    def upload_to_s3(downloaded_file_path)
      s3 = Aws::S3::Resource.new

      extension = File.extname(@meta.cmc_logo_url)
      file_name = File.join(BUCKET_PATH, "#{@meta.coin_id}#{extension}")

      object = s3.bucket(BUCKET).object(file_name)
      object.upload_file(downloaded_file_path, acl: 'public-read')

      object.public_url
    end
  end
end
