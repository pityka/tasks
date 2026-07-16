// The EC2 backend is split into cohesive files. See:
//   - EC2Config.scala        — HOCON config
//   - EC2Metadata.scala      — IMDSv2 access
//   - EC2ClientBuilder.scala — smithy4s EC2 client
//   - EC2Operations.scala    — EC2 API wrappers
//   - EC2UserData.scala      — worker userdata script
//   - EC2ElasticSupport.scala — ElasticSupport entry + Shutdown/CreateNode/GetNodeName
package tasks.elastic.ec2
