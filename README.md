## Monitor QLDB Access Patterns
This project accompanies the AWS Blog post [Monitor Amazon QLDB Access Patterns](https://aws.amazon.com/blogs/database/monitor-amazon-qldb-query-access-patterns/).

### Deploying this repository

Please note that deploying this repository may incur cost. There are instructions at the bottom of this README on how to clean up the assets that are created.

This project contains a [AWS Serverless Application Model](https://aws.amazon.com/serverless/sam/) template that deploys the components described in the blog post. To deploy this template in your AWS account:

#### From the command line
1. Install the latest version of the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
2. Clone this repository to your local machine `git clone https://github.com/aws-samples/qldb-monitor-access-patterns-blog-post`
3. Run the following command to deploy the template: `sam deploy --guided` For Stack Name enter `qldb-monitor-access-patterns-blog-post`. Use defaults for everything else.

### Cleaning up

To remove the resources created by this repository:

#### From the command line
1. Run the following command: `aws sam delete --stack-name qldb-monitor-access-patterns-blog-post`

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
