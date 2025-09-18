This is a demo application to show how one could synchronize (copy) content from one Azure File Share to another Azure File Share and keep the content in sync.
Provide the source and destination Storage Account- and File Share details and run the application.
Next time you run it, it will look for changes and 'sync' only the changes.

It uses server-side copies, so the file data doesn't traverse your client that runs the app.
