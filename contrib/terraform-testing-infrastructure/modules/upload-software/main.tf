variable local_sources_dir {}
variable upload_dir {}
variable upload_host {}

resource "null_resource" "upload_software" {
  count = var.local_sources_dir == "" ? 0 : 1
  connection {
    type = "ssh"
    host = var.upload_host
    user = "hadoop"
  }
  provisioner "file" {
    source      = var.local_sources_dir
    destination = "${var.upload_dir}/sources"
  }
}
