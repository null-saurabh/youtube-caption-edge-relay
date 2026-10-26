import 'package:flutter_test/flutter_test.dart';
import 'package:youtube_caption_edge_relay/main.dart';

void main() {
  testWidgets('Relay dashboard renders', (WidgetTester tester) async {
    await tester.pumpWidget(const CaptionEdgeRelayApp());
    expect(find.text('YouTube Caption Edge Relay'), findsOneWidget);
  });
}
