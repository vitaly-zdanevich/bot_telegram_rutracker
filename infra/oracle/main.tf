data "oci_identity_availability_domains" "available" {
  compartment_id = var.oci_tenancy_ocid
}

data "oci_core_images" "ubuntu" {
  compartment_id           = var.oci_compartment_ocid
  operating_system         = var.oracle_image_operating_system
  operating_system_version = var.oracle_image_operating_system_version
  shape                    = var.oracle_shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
  state                    = "AVAILABLE"
}

locals {
  availability_domain = var.availability_domain != "" ? var.availability_domain : data.oci_identity_availability_domains.available.availability_domains[0].name
  oracle_image_ocid   = var.oracle_image_ocid != "" && var.oracle_image_ocid != "ocid1.image.oc1..replace" ? var.oracle_image_ocid : data.oci_core_images.ubuntu.images[0].id
  env = {
    telegram_bot_token                         = jsonencode(var.telegram_bot_token)
    telegram_webhook_secret                    = jsonencode(var.telegram_webhook_secret)
    allowed_telegram_user_ids                  = jsonencode(var.allowed_telegram_user_ids)
    rutracker_base_urls                        = jsonencode(var.rutracker_base_urls)
    image_cache_public_base_url                = jsonencode(var.image_cache_public_base_url)
    image_cache_dir                            = jsonencode(var.image_cache_dir)
    rutracker_username                         = jsonencode(var.rutracker_username)
    rutracker_password                         = jsonencode(var.rutracker_password)
    rutracker_cookie                           = jsonencode(var.rutracker_cookie)
    max_file_mb                                = jsonencode(tostring(var.max_file_mb))
    download_timeout_seconds                   = jsonencode(tostring(var.download_timeout_seconds))
    download_margin_seconds                    = jsonencode(tostring(var.download_margin_seconds))
    torrent_peer_limit                         = jsonencode(tostring(var.torrent_peer_limit))
    torrent_listen_port                        = jsonencode(tostring(var.torrent_listen_port))
    seed_disk_reserve_mb                       = jsonencode(tostring(var.seed_disk_reserve_mb))
    rutracker_catalog_enabled                  = jsonencode(tostring(var.rutracker_catalog_enabled))
    rutracker_catalog_path                     = jsonencode(var.rutracker_catalog_path)
    rutracker_catalog_xml_topic_id             = jsonencode(tostring(var.rutracker_catalog_xml_topic_id))
    rutracker_catalog_download_timeout_seconds = jsonencode(tostring(var.rutracker_catalog_download_timeout_seconds))
  }
}

resource "oci_core_vcn" "bot" {
  compartment_id = var.oci_compartment_ocid
  cidr_block     = var.vcn_cidr
  display_name   = "${var.project_name}-vcn"
  dns_label      = "rutrackerbot"
}

resource "oci_core_internet_gateway" "bot" {
  compartment_id = var.oci_compartment_ocid
  display_name   = "${var.project_name}-igw"
  enabled        = true
  vcn_id         = oci_core_vcn.bot.id
}

resource "oci_core_route_table" "bot" {
  compartment_id = var.oci_compartment_ocid
  display_name   = "${var.project_name}-routes"
  vcn_id         = oci_core_vcn.bot.id

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.bot.id
  }
}

resource "oci_core_security_list" "bot" {
  compartment_id = var.oci_compartment_ocid
  display_name   = "${var.project_name}-security"
  vcn_id         = oci_core_vcn.bot.id

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  ingress_security_rules {
    protocol = "6"
    source   = var.ssh_ingress_cidr

    tcp_options {
      min = 22
      max = 22
    }
  }

  ingress_security_rules {
    protocol = "6"
    source   = var.vm_worker_ingress_cidr

    tcp_options {
      min = 8080
      max = 8080
    }
  }

  ingress_security_rules {
    protocol = "6"
    source   = var.vm_worker_ingress_cidr

    tcp_options {
      min = 80
      max = 80
    }
  }

  ingress_security_rules {
    protocol = "6"
    source   = var.torrent_ingress_cidr

    tcp_options {
      min = var.torrent_listen_port
      max = var.torrent_listen_port
    }
  }

  ingress_security_rules {
    protocol = "17"
    source   = var.torrent_ingress_cidr

    udp_options {
      min = var.torrent_listen_port
      max = var.torrent_listen_port
    }
  }
}

resource "oci_core_subnet" "bot" {
  cidr_block                 = var.subnet_cidr
  compartment_id             = var.oci_compartment_ocid
  display_name               = "${var.project_name}-subnet"
  dns_label                  = "bot"
  prohibit_public_ip_on_vnic = false
  route_table_id             = oci_core_route_table.bot.id
  security_list_ids          = [oci_core_security_list.bot.id]
  vcn_id                     = oci_core_vcn.bot.id
}

resource "oci_core_instance" "bot" {
  availability_domain = local.availability_domain
  compartment_id      = var.oci_compartment_ocid
  display_name        = var.project_name
  shape               = var.oracle_shape

  shape_config {
    memory_in_gbs = var.oracle_memory_gb
    ocpus         = var.oracle_ocpus
  }

  create_vnic_details {
    assign_public_ip = true
    display_name     = "${var.project_name}-vnic"
    hostname_label   = "bot"
    subnet_id        = oci_core_subnet.bot.id
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data = base64encode(templatefile("${path.module}/cloud-init.yaml.tftpl", {
      project_name                               = var.project_name
      telegram_api_id                            = var.telegram_api_id
      telegram_api_hash                          = jsonencode(var.telegram_api_hash)
      vm_worker_secret                           = jsonencode(var.vm_worker_secret)
      bot_repo_url                               = jsonencode(var.bot_repo_url)
      bot_repo_ref                               = jsonencode(var.bot_repo_ref)
      telegram_bot_token                         = local.env.telegram_bot_token
      telegram_webhook_secret                    = local.env.telegram_webhook_secret
      allowed_telegram_user_ids                  = local.env.allowed_telegram_user_ids
      rutracker_base_urls                        = local.env.rutracker_base_urls
      image_cache_public_base_url                = local.env.image_cache_public_base_url
      image_cache_dir                            = local.env.image_cache_dir
      rutracker_username                         = local.env.rutracker_username
      rutracker_password                         = local.env.rutracker_password
      rutracker_cookie                           = local.env.rutracker_cookie
      max_file_mb                                = local.env.max_file_mb
      download_timeout_seconds                   = local.env.download_timeout_seconds
      download_margin_seconds                    = local.env.download_margin_seconds
      torrent_peer_limit                         = local.env.torrent_peer_limit
      torrent_listen_port                        = local.env.torrent_listen_port
      seed_disk_reserve_mb                       = local.env.seed_disk_reserve_mb
      rutracker_catalog_enabled                  = local.env.rutracker_catalog_enabled
      rutracker_catalog_path                     = local.env.rutracker_catalog_path
      rutracker_catalog_xml_topic_id             = local.env.rutracker_catalog_xml_topic_id
      rutracker_catalog_download_timeout_seconds = local.env.rutracker_catalog_download_timeout_seconds
    }))
  }

  source_details {
    source_id               = local.oracle_image_ocid
    source_type             = "image"
    boot_volume_size_in_gbs = var.oracle_boot_volume_gb
  }

  lifecycle {
    ignore_changes = [metadata["user_data"]]
  }
}
