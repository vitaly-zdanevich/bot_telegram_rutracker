output "oracle_public_ip" {
  description = "Public IP address of the Oracle VM."
  value       = oci_core_instance.bot.public_ip
}

output "ssh_command" {
  description = "SSH command for the Oracle VM."
  value       = "ssh ubuntu@${oci_core_instance.bot.public_ip}"
}

output "vm_worker_url" {
  description = "Signed VM worker endpoint to configure as vm_worker_url in the Lambda stack."
  value       = "http://${oci_core_instance.bot.public_ip}:8080/telegram"
}
